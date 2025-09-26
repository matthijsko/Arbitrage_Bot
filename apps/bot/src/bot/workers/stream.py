import asyncio
import os
import time
import importlib.util
from typing import List, Tuple
import orjson
from redis.asyncio import from_url as redis_from_url
from ..services.markets import get_exchange
from ..services.symbols import resolve_symbol_for_exchange

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STREAM_EXCHANGES = [x.strip().lower() for x in os.getenv("STREAM_EXCHANGES", "bitvavo,coinbase,kraken").split(",") if x.strip()]
STREAM_SYMBOLS = [x.strip() for x in os.getenv("STREAM_SYMBOLS", "BTC/EUR,ETH/EUR").split(",") if x.strip()]
ORDERBOOK_DEPTH = int(float(os.getenv("ORDERBOOK_DEPTH", "50")))
REST_POLL_SEC = float(os.getenv("REST_POLL_SEC", "2.0"))

def _sanitize_levels(levels):
    out = []
    for lvl in levels or []:
        try:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                price, amount = float(lvl[0]), float(lvl[1])
            elif isinstance(lvl, dict):
                price = float(lvl.get("price") or lvl.get("p") or lvl.get(0))
                amount = float(lvl.get("amount") or lvl.get("volume") or lvl.get("a") or lvl.get(1))
            else:
                continue
            if amount > 0:
                out.append((price, amount))
        except Exception:
            continue
    return out

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

async def stream_with_ccxtpro(r, exchange: str, symbol: str):
    import importlib.util, time
    spec = importlib.util.find_spec("ccxt.pro")
    if spec is None:
        return False  # ccxt.pro niet geïnstalleerd → REST fallback
    import ccxt.pro as ccxtpro

    ex = getattr(ccxtpro, exchange)({"enableRateLimit": True, "timeout": 20000})
    try:
        real_sym = resolve_symbol_for_exchange(ex, symbol)  # bv. BTC/EUR → XBT/EUR op Kraken
        while True:
            ob = await ex.watch_order_book(real_sym, limit=ORDERBOOK_DEPTH)
            asks = _sanitize_levels(ob.get("asks"))
            bids = _sanitize_levels(ob.get("bids"))
            ts = ob.get("timestamp") or int(time.time() * 1000)
            await publish_orderbook(r, exchange, symbol, asks, bids, ts)
    finally:
        try:
            await ex.close()
        except Exception:
            pass
    return True

async def poll_with_ccxt(r, exchange: str, symbol: str):
    import ccxt, time, asyncio
    ex = getattr(ccxt, exchange)({"enableRateLimit": True, "timeout": 15000})
    real_sym = resolve_symbol_for_exchange(ex, symbol)
    while True:
        try:
            ob = ex.fetch_order_book(real_sym, limit=ORDERBOOK_DEPTH)
            asks = _sanitize_levels(ob.get("asks"))
            bids = _sanitize_levels(ob.get("bids"))
            ts = ob.get("timestamp") or int(time.time() * 1000)
            await publish_orderbook(r, exchange, symbol, asks, bids, ts)
        except Exception:
            await asyncio.sleep(REST_POLL_SEC * 2)
        await asyncio.sleep(REST_POLL_SEC)

async def run_pair(r, exchange: str, symbol: str):
    if not await stream_with_ccxtpro(r, exchange, symbol):
        await poll_with_ccxt(r, exchange, symbol)

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
    
