from typing import Dict, Any, List
from .exchanges import fetch_orderbook, get_market_meta
from .depth_sim import simulate_cross_fill

def compute_pair_opportunity(
    symbol: str,
    buy_ex: str,
    sell_ex: str,
    budget_quote: float = 100.0,
    withdraw_fee_base: float = 0.0,
) -> Dict[str, Any]:
    asks, _ = fetch_orderbook(buy_ex, symbol, limit=50)
    _, bids = fetch_orderbook(sell_ex, symbol, limit=50)

    if not asks or not bids:
        return {"ok": 0, "reason": "empty_orderbook", "buy": buy_ex, "sell": sell_ex, "symbol": symbol}

    best_ask = asks[0][0]
    best_bid = bids[0][0]
    gross_spread = (best_bid - best_ask) / best_ask

    buy_meta = get_market_meta(buy_ex, symbol)
    sell_meta = get_market_meta(sell_ex, symbol)
    fee_buy = buy_meta["taker_fee"]
    fee_sell = sell_meta["taker_fee"]

    res = simulate_cross_fill(
        asks=asks,
        bids=bids,
        fee_buy=fee_buy,
        fee_sell=fee_sell,
        withdraw_fee_base=withdraw_fee_base,
        max_quote_buy=budget_quote,
        base_step=buy_meta.get("base_step") or sell_meta.get("base_step"),
        min_base=buy_meta.get("min_base") or sell_meta.get("min_base"),
        min_notional_buy=buy_meta.get("min_notional"),
        min_notional_sell=sell_meta.get("min_notional"),
    )

    return {
        "ok": res.get("ok", 0),
        "symbol": symbol,
        "buy": buy_ex,
        "sell": sell_ex,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "gross_spread": gross_spread,
        "fee_buy": fee_buy,
        "fee_sell": fee_sell,
        "budget_quote": budget_quote,
        "withdraw_fee_base": withdraw_fee_base,
        "depth_result": res,
    }

def compute_all_pairs(symbol: str, exchanges: List[str], budget_quote: float, withdraw_fee_base: float) -> List[Dict[str, Any]]:
    out = []
    for i, buy_ex in enumerate(exchanges):
        for j, sell_ex in enumerate(exchanges):
            if i == j:
                continue
            try:
                out.append(compute_pair_opportunity(symbol, buy_ex, sell_ex, budget_quote, withdraw_fee_base))
            except Exception as e:
                out.append({"ok": 0, "symbol": symbol, "buy": buy_ex, "sell": sell_ex, "error": str(e)})
    out.sort(key=lambda x: (x.get("depth_result", {}).get("net_profit_quote") or -1e18), reverse=True)
    return out
