import orjson
from redis.asyncio import Redis
from ..schemas.opportunity import Opportunity

async def fetch_recent_opportunities(redis: Redis, list_key: str, limit: int = 20) -> list[Opportunity]:
    items = await redis.lrange(list_key, 0, max(0, limit - 1))
    out: list[Opportunity] = []
    for raw in items:
        try:
            data = orjson.loads(raw)
            out.append(Opportunity(**data))
        except Exception:
            continue
    return out
