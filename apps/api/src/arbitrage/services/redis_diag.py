import os, time, orjson
from typing import Any, Dict, List
from redis.asyncio import from_url as redis_from_url

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

async def keys(pattern: str) -> List[str]:
    r = redis_from_url(REDIS_URL, decode_responses=True)
    try:
        return [k async for k in r.scan_iter(match=pattern)]
    finally:
        await r.close()

async def get_json(key: str) -> Dict[str, Any]:
    r = redis_from_url(REDIS_URL, decode_responses=False)
    try:
        raw = await r.get(key)
        if not raw:
            return {"key": key, "exists": False}
        snap = orjson.loads(raw)
        age_ms = None
        ts = snap.get("ts")
        if ts:
            try:
                age_ms = int(time.time() * 1000) - int(ts)
            except Exception:
                age_ms = None
        return {"key": key, "exists": True, "age_ms": age_ms, "data": snap}
    finally:
        await r.close()
