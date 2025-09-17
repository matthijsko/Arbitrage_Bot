from fastapi import APIRouter, Depends, Query
from redis.asyncio import Redis, from_url as redis_from_url
from typing import List, Optional

from ..config import settings
from ..schemas.opportunity import Opportunity
from ..services.redis_store import fetch_recent_opportunities

router = APIRouter(prefix="/opportunities", tags=["opportunities"])

async def get_redis_client() -> Redis:
    return redis_from_url(settings.redis_url, decode_responses=True)

@router.get("", response_model=List[Opportunity])
async def list_opportunities(
    limit: int = Query(20, ge=1, le=100),
    symbol: Optional[str] = Query(None, description="Filter by symbol (e.g., BTC/USDT)"),
    min_profit: Optional[float] = Query(None, description="Minimum net profit in quote"),
    min_spread_bps: Optional[float] = Query(None, description="Minimum gross spread in bps"),
    redis: Redis = Depends(get_redis_client),
):
    items = await fetch_recent_opportunities(redis, settings.opp_list_key, limit=limit)
    if symbol:
        items = [x for x in items if x.symbol == symbol]
    if min_profit is not None:
        items = [x for x in items if x.net_profit_quote >= min_profit]
    if min_spread_bps is not None:
        items = [x for x in items if x.spread_bps >= min_spread_bps]
    return items
