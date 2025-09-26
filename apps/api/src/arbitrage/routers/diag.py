from fastapi import APIRouter, Query
from ..services.redis_diag import keys as redis_keys, get_json as redis_get

router = APIRouter(prefix="/diag", tags=["diagnostics"])

@router.get("/redis/keys")
async def redis_list(pattern: str = Query("ob:*")):
    ks = await redis_keys(pattern)
    return {"pattern": pattern, "count": len(ks), "keys": ks}

@router.get("/redis/get")
async def redis_get_key(key: str):
    return await redis_get(key)
