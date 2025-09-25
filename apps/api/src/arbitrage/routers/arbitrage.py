from fastapi import APIRouter, Query
from ..services.arbitrage import compute_all_pairs

router = APIRouter(prefix="/arbitrage", tags=["arbitrage"])

@router.get("/scan")
async def scan_arbitrage(
    symbol: str = Query(..., examples=["BTC/EUR"]),
    exchanges: str = Query("bitvavo,coinbase,kraken", description="comma separated list of exchanges"),
    budget_quote: float = Query(250.0, ge=1.0, description="Budget in quote currency for the buy leg"),
    withdraw_fee_base: float = Query(0.0, ge=0.0, description="Base asset withdrawal fee subtracted after buy"),
):
    ex_list = [e.strip().lower() for e in exchanges.split(",") if e.strip()]
    results = await compute_all_pairs(symbol, ex_list, budget_quote, withdraw_fee_base)
    return {"symbol": symbol, "exchanges": ex_list, "results": results}
