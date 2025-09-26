from fastapi import APIRouter, Query
from typing import Optional
from ..services.orderbook_store import get_cached_orderbook
from ..services.exchanges import (
    fetch_orderbook, fetch_ticker, list_symbols, list_symbols_with_quote,
    get_market_meta, ping
)

router = APIRouter(prefix="/markets", tags=["markets"])

@router.get("/symbols")
def symbols(exchange: str, quote: Optional[str] = Query(None, description="Filter op quote, bv EUR of USDT")):
    return {
        "exchange": exchange,
        "quote": quote,
        "symbols": list_symbols_with_quote(exchange, quote) if quote else list_symbols(exchange),
    }

@router.get("/status")
def status(exchange: str):
    return {"exchange": exchange, "status": ping(exchange)}

@router.get("/meta")
def meta(exchange: str, symbol: str):
    return {"exchange": exchange, "symbol": symbol, "meta": get_market_meta(exchange, symbol)}

@router.get("/ticker")
def ticker(exchange: str, symbol: str):
    return {"exchange": exchange, "symbol": symbol, "ticker": fetch_ticker(exchange, symbol)}

@router.get("/orderbook")
async def orderbook(
    exchange: str,
    symbol: str,
    limit: int = Query(25, ge=1, le=200),
    prefer: str = Query("cache", description="cache|rest")
):
    asks = bids = None
    source = None
    if prefer in ("cache", "any", "auto"):
        cached = await get_cached_orderbook(exchange, symbol)
        if cached:
            a, b = cached
            asks, bids = a[:limit], b[:limit]
            source = "cache"
    if asks is None or bids is None:
        a, b = fetch_orderbook(exchange, symbol, limit=limit)
        asks, bids = a, b
        source = "rest"
    return {"exchange": exchange, "symbol": symbol, "asks": asks, "bids": bids, "source": source}
