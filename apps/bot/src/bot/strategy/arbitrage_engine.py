import os, time, orjson, asyncio
from typing import Dict, Any, List
from ..services.orderbook_store import get_cached_orderbook
from ..services.markets import fetch_orderbook, get_market_meta
from .depth_sim import simulate_cross_fill
from redis.asyncio import from_url as redis_from_url

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "opps")
PUBLISH_STREAM = os.getenv("PUBLISH_STREAM", "opps_stream")

def _now_ms() -> int:
    return int(time.time() * 1000)

async def compute_pair(
    symbol: str, buy_ex: str, sell_ex: str,
    budget_quote: float, withdraw_fee_base: float
) -> Dict[str, Any]:
    cached_buy = await get_cached_orderbook(buy_ex, symbol)
    cached_sell = await get_cached_orderbook(sell_ex, symbol)
    if cached_buy:
        asks, _ = cached_buy
    else:
        asks, _ = fetch_orderbook(buy_ex, symbol, limit=50)
    if cached_sell:
        _, bids = cached_sell
    else:
        _, bids = fetch_orderbook(sell_ex, symbol, limit=50)
    if not asks or not bids:
        return {"ok": 0, "reason": "empty_orderbook", "symbol": symbol, "buy": buy_ex, "sell": sell_ex}

    buy_meta = get_market_meta(buy_ex, symbol)
    sell_meta = get_market_meta(sell_ex, symbol)
    fee_buy, fee_sell = buy_meta["taker_fee"], sell_meta["taker_fee"]

    best_ask, best_bid = asks[0][0], bids[0][0]
    gross_spread = (best_bid - best_ask) / best_ask

    res = simulate_cross_fill(
        asks=asks, bids=bids,
        fee_buy=fee_buy, fee_sell=fee_sell,
        withdraw_fee_base=withdraw_fee_base,
        max_quote_buy=budget_quote,
        base_step=buy_meta.get("base_step") or sell_meta.get("base_step"),
        min_base=buy_meta.get("min_base") or sell_meta.get("min_base"),
        min_notional_buy=buy_meta.get("min_notional"),
        min_notional_sell=sell_meta.get("min_notional"),
    )

    return {
        "ok": res.get("ok", 0),
        "ts": _now_ms(),
        "symbol": symbol,
        "buy": buy_ex,
        "sell": sell_ex,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "gross_spread": gross_spread,
        "depth": res,
    }

async def scan_all(
    symbol: str, exchanges: List[str], budget_quote: float, withdraw_fee_base: float
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, bx in enumerate(exchanges):
        for j, sx in enumerate(exchanges):
            if i == j:
                continue
            try:
                out.append(await compute_pair(symbol, bx, sx, budget_quote, withdraw_fee_base))
            except Exception as e:
                out.append({"ok": 0, "symbol": symbol, "buy": bx, "sell": sx, "error": str(e)})
    out.sort(key=lambda x: (x.get("depth", {}).get("net_profit_quote") or -1e18), reverse=True)
    return out

async def publish_opportunities(items: List[Dict[str, Any]], topn: int = 5):
    if not items:
        return
    r = redis_from_url(REDIS_URL, decode_responses=False)
    try:
        payload = orjson.dumps({"ts": _now_ms(), "items": items[:topn]})
        await r.publish(PUBLISH_CHANNEL, payload)  # Pub/Sub realtime
        await r.xadd(PUBLISH_STREAM, {"payload": payload}, maxlen=1000, approximate=True)  # Stream history
    finally:
        await r.close()

async def run_strategy_once(symbols: List[str], exchanges: List[str],
                            budget_quote: float, withdraw_fee_base: float,
                            min_net_quote: float, min_roi_pct: float, topn: int) -> Dict[str, Any]:
    """
    Returns:
      {
        "ts": <ms>,
        "blocks": [
           {
             "symbol": "BTC/EUR",
             "best": <beste na filters>,            # kan ontbreken als geen winstgevende
             "top": [<topN na filters>],            # kan leeg zijn
             "debug_top": [<topN ongefilterd>],     # altijd aanwezig (beste net_profit eerst)
             "debug_best_any": <beste ongefilterd>  # altijd 1 element of None
           },
           ...
        ]
      }
    """
    blocks: List[Dict[str, Any]] = []

    for sym in symbols:
        pairs = await scan_all(sym, exchanges, budget_quote, withdraw_fee_base)
        # ongefilterd op net_profit sorteren (al gedaan in scan_all)
        debug_top = pairs[:topn]
        debug_best_any = next((p for p in pairs if p.get("ok")), None)

        # drempels toepassen
        filtered = []
        for p in pairs:
            if not p.get("ok"):
                continue
            d = p.get("depth", {}) or {}
            net = d.get("net_profit_quote") or 0.0
            roi = d.get("roi") or 0.0
            if net >= min_net_quote and (roi * 100.0) >= min_roi_pct:
                filtered.append(p)
        filtered.sort(key=lambda x: x.get("depth", {}).get("net_profit_quote") or -1e18, reverse=True)

        block: Dict[str, Any] = {
            "symbol": sym,
            "top": filtered[:topn],
            "debug_top": debug_top,
            "debug_best_any": debug_best_any,
        }
        if filtered:
            block["best"] = filtered[0]

        blocks.append(block)

    # Publiceer platte lijst (gefilterd) voor downstream consumers
    flat = []
    for b in blocks:
        flat.extend(b.get("top") or [])
    await publish_opportunities(flat, topn=topn)

    return {"ts": _now_ms(), "blocks": blocks}
