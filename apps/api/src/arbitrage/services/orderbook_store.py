import os, time, orjson
from typing import Optional, Tuple, List
from redis.asyncio import from_url as redis_from_url

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STALE_MS = int(float(os.getenv("ORDERBOOK_STALE_MS", "5000")))

def _key(exchange: str, symbol: str) -> str:
    return f"ob:{exchange}:{symbol}"

async def get_cached_orderbook(exchange: str, symbol: str) -> Optional[Tuple[List[tuple], List[tuple]]]:
    redis = redis_from_url(REDIS_URL, decode_responses=False)
    try:
        data = await redis.get(_key(exchange, symbol))
        if not data:
            return None
        snap = orjson.loads(data)
        ts = int(snap.get("ts") or 0)
        if ts and (time.time()*1000 - ts) > STALE_MS:
            return None
        asks = [(float(p), float(a)) for p,a in snap.get("asks", [])]
        bids = [(float(p), float(a)) for p,a in snap.get("bids", [])]
        asks.sort(key=lambda x: x[0])
        bids.sort(key=lambda x: x[0], reverse=True)
        return asks, bids
    finally:
        await redis.close()
