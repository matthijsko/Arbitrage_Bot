from fastapi import APIRouter, Query
from typing import List, Dict, Any
import asyncio
from ..services.arbitrage import compute_all_pairs
from ..services.exchanges import list_symbols_with_quote

router = APIRouter(prefix="/arbitrage", tags=["arbitrage"])

@router.get("/scan")
async def scan_arbitrage(
    symbol: str = Query(..., examples=["BTC/EUR"]),
    exchanges: str = Query("bitvavo,coinbase,kraken"),
    budget_quote: float = Query(250.0, ge=1.0),
    withdraw_fee_base: float = Query(0.0, ge=0.0),
):
    ex_list = [e.strip().lower() for e in exchanges.split(",") if e.strip()]
    results = await compute_all_pairs(symbol, ex_list, budget_quote, withdraw_fee_base)
    return {"symbol": symbol, "exchanges": ex_list, "results": results}

@router.get("/scan-multi")
async def scan_multi(
    symbols: str = Query("BTC/EUR,ETH/EUR"),
    exchanges: str = Query("bitvavo,coinbase,kraken"),
    budget_quote: float = Query(250.0, ge=1.0),
    withdraw_fee_base: float = Query(0.0, ge=0.0),
):
    ex_list = [e.strip().lower() for e in exchanges.split(",") if e.strip()]
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    tasks = [compute_all_pairs(sym, ex_list, budget_quote, withdraw_fee_base) for sym in sym_list]
    blocks: List[List[Dict[str, Any]]] = await asyncio.gather(*tasks, return_exceptions=False)
    out = []
    for sym, res in zip(sym_list, blocks):
        out.append({"symbol": sym, "results": res})
    return {"exchanges": ex_list, "blocks": out}

@router.get("/discover")
def discover(
    exchanges: str = Query("bitvavo,coinbase,kraken"),
    quote: str = Query("EUR")
):
    ex_list = [e.strip().lower() for e in exchanges.split(",") if e.strip()]
    quote = quote.upper()
    return {ex: list_symbols_with_quote(ex, quote) for ex in ex_list}
