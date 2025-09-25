import asyncio
import os
import time
import importlib.util
from typing import List, Tuple
import orjson
from redis.asyncio import from_url as redis_from_url

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STREAM_EXCHANGES = [x.strip().lower() for x in os.getenv("STREAM_EXCHANGES", "bitvavo,coinbase,kraken").split(",") if x.strip()]
STREAM_SYMBOLS = [x.strip() for x in os.getenv("STREAM_SYMBOLS", "BTC/EUR,ETH/EUR").split(",") if x.strip()]
ORDERBOOK_DEPTH = int(float(os.getenv("ORDERBOOK_DEPTH", "50")))
REST_POLL_SEC = float(os.getenv("REST_POLL_SEC", "2.0"))

def _key(exchange: str, symbol: str) -> str:
    return f"ob:{exchange}:{symbol}"

async def publish_orderbook(redis, exchange: str, symbol: str, asks: List[Tuple[float,float]], bids: List[Tuple[float,float]], ts_ms: int | None):
    payload = {
        "exchange": exchange,
        "symbol": symbol,
        "ts": int(ts_ms or time.time()*1000),
        "asks": asks[:ORDERBOOK_DEPTH],
        "bids": bids[:ORDERBOOK_DEPTH],
    }
    data = orjson.dumps(payload)
    # TTL kort, zodat API staleness kan herkennen
    await redis.set(_key(exchange, symbol), data, ex=10)

async def stream_with_ccxtpro(redis, exchange: str, symbol: str):
    spec = importlib.util.find_spec("ccxt.pro")
    if spec is None:
        return False
    import ccxt.pro as ccxtpro
    klass = getattr(ccxtpro, exchange)
    ex = klass({"enableRateLimit": True, "timeout": 20000})
    try:
        while True:
            ob = await ex.watch_order_book(symbol, limit=ORDERBOOK_DEPTH)
            asks = [(float(p), float(a)) for p,a in ob.get("asks", []) if float(a) > 0]
            bids = [(float(p), float(a)) for p,a in ob.get("bids", []) if float(a) > 0]
            ts = ob.get("timestamp") or int(time.time()*1000)
            await publish_orderbook(redis, exchange, symbol, asks, bids, ts)
    finally:
        try:
            await ex.close()
        except Exception:
            pass
    return True

async def poll_with_ccxt(redis, exchange: str, symbol: str):
    import ccxt
    klass = getattr(ccxt, exchange)
    ex = klass({"enableRateLimit": True, "timeout": 15000})
    while True:
        try:
            ob = ex.fetch_order_book(symbol, limit=ORDERBOOK_DEPTH)
            asks = [(float(p), float(a)) for p,a in ob.get("asks", []) if float(a) > 0]
            bids = [(float(p), float(a)) for p,a in ob.get("bids", []) if float(a) > 0]
            ts = ob.get("timestamp") or int(time.time()*1000)
            await publish_orderbook(redis, exchange, symbol, asks, bids, ts)
        except Exception:
            # kleine backoff bij fout
            await asyncio.sleep(REST_POLL_SEC * 2)
        await asyncio.sleep(REST_POLL_SEC)

async def run_pair(redis, exchange: str, symbol: str):
    used_ws = await stream_with_ccxtpro(redis, exchange, symbol)
    if not used_ws:
        await poll_with_ccxt(redis, exchange, symbol)

async def run():
    redis = redis_from_url(REDIS_URL, decode_responses=False)
    tasks = []
    for ex in STREAM_EXCHANGES:
        for sym in STREAM_SYMBOLS:
            tasks.append(asyncio.create_task(run_pair(redis, ex, sym)))
    try:
        await asyncio.gather(*tasks)
    finally:
        await redis.close()
