from fastapi import APIRouter, Query
from ..services.orderbook_store import get_cached_orderbook
from ..services.exchanges import fetch_orderbook

router = APIRouter(prefix="/markets", tags=["markets"])

@router.get("/orderbook")
async def get_orderbook(
    exchange: str = Query(...), symbol: str = Query(...), limit: int = Query(25, ge=1, le=200)
):
    cached = await get_cached_orderbook(exchange, symbol)
    if cached:
        asks, bids = cached
        return {"exchange": exchange, "symbol": symbol, "asks": asks[:limit], "bids": bids[:limit], "source": "cache"}
    asks, bids = fetch_orderbook(exchange, symbol, limit=limit)
    return {"exchange": exchange, "symbol": symbol, "asks": asks, "bids": bids, "source": "rest"}
