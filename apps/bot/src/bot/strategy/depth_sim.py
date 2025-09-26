import math
from typing import List, Tuple, Optional, Dict

def _floor_step(value: float, step: Optional[float]) -> float:
    if not step or step <= 0:
        return value
    return math.floor(value / step) * step

def _ceil_step(value: float, step: Optional[float]) -> float:
    if not step or step <= 0:
        return value
    return math.ceil(value / step) * step

def simulate_cross_fill(
    asks: List[Tuple[float, float]],  # [(price, size_base)] low->high
    bids: List[Tuple[float, float]],  # [(price, size_base)] high->low
    fee_buy: float = 0.001,
    fee_sell: float = 0.001,
    withdraw_fee_base: float = 0.0,
    max_quote_buy: Optional[float] = None,
    max_base_sell: Optional[float] = None,
    base_step: Optional[float] = None,
    min_base: Optional[float] = None,
    min_notional_buy: Optional[float] = None,
    min_notional_sell: Optional[float] = None
) -> Dict[str, float]:
    asks = [(float(p), float(s)) for p, s in asks if s > 0]
    bids = [(float(p), float(s)) for p, s in bids if s > 0]
    if not asks or not bids:
        return {"qty_base_bought": 0.0, "qty_base_sold": 0.0, "net_profit_quote": 0.0, "ok": 0}
    asks = sorted(asks, key=lambda x: x[0])
    bids = sorted(bids, key=lambda x: x[0], reverse=True)

    spent_quote = 0.0
    acquired_base = 0.0
    buy_fee_quote = 0.0

    # BUY across asks
    for ask_px, ask_sz in asks:
        max_affordable_base = float("inf") if max_quote_buy is None else max(0.0, (max_quote_buy - spent_quote) / ask_px)
        take_base = min(ask_sz, max_affordable_base)
        if base_step:
            take_base = _floor_step(take_base, base_step)
        if take_base <= 0:
            break
        notional = take_base * ask_px
        if min_notional_buy and notional < min_notional_buy:
            need_base = (min_notional_buy / ask_px)
            need_base = max(need_base, min_base or 0.0)
            if base_step:
                need_base = _ceil_step(need_base, base_step)
            if need_base <= ask_sz and (max_quote_buy is None or spent_quote + need_base * ask_px <= max_quote_buy):
                take_base = need_base
                notional = take_base * ask_px
            else:
                continue
        if min_base and take_base < min_base:
            tb = min(ask_sz, max_affordable_base, min_base)
            if base_step:
                tb = _ceil_step(tb, base_step)
            if tb <= ask_sz and (max_quote_buy is None or spent_quote + tb * ask_px <= max_quote_buy):
                take_base = tb
                notional = take_base * ask_px
            else:
                continue
        spent_quote += notional
        buy_fee_quote += notional * fee_buy
        acquired_base += take_base
        if max_quote_buy is not None and spent_quote >= max_quote_buy - 1e-12:
            break

    if max_base_sell is not None:
        acquired_base = min(acquired_base, max_base_sell)

    transferable_base = max(0.0, acquired_base - withdraw_fee_base)

    # SELL across bids
    remaining_base_to_sell = transferable_base
    received_quote = 0.0
    sell_fee_quote = 0.0
    qty_sold = 0.0

    for bid_px, bid_sz in bids:
        if remaining_base_to_sell <= 0:
            break
        take_base = min(bid_sz, remaining_base_to_sell)
        notional = take_base * bid_px
        if min_notional_sell and notional < min_notional_sell:
            need_base = (min_notional_sell / bid_px)
            if base_step:
                need_base = _ceil_step(need_base, base_step)
            need_base = min(need_base, remaining_base_to_sell, bid_sz)
            notional = need_base * bid_px
            if need_base <= 0 or (min_base and need_base < min_base):
                continue
            take_base = need_base

        if base_step:
            take_base = _floor_step(take_base, base_step)
            if take_base <= 0:
                continue

        notional = take_base * bid_px
        fee = notional * fee_sell
        received_quote += notional - fee
        sell_fee_quote += fee
        remaining_base_to_sell -= take_base
        qty_sold += take_base

    if acquired_base <= 0 or qty_sold <= 0:
        return {
            "qty_base_bought": float(acquired_base),
            "qty_base_sold": float(qty_sold),
            "avg_buy_px": asks[0][0],
            "avg_sell_px": bids[0][0],
            "spent_quote": float(spent_quote),
            "received_quote": float(received_quote),
            "buy_fee_quote": float(buy_fee_quote),
            "sell_fee_quote": float(sell_fee_quote),
            "withdraw_fee_base": float(withdraw_fee_base),
            "net_profit_quote": float(received_quote - spent_quote - buy_fee_quote),
            "ok": 0
        }

    avg_buy_px = (spent_quote / acquired_base) if acquired_base > 0 else 0.0
    avg_sell_px = (received_quote + sell_fee_quote) / qty_sold if qty_sold > 0 else 0.0

    net_profit = received_quote - spent_quote - buy_fee_quote
    roi = net_profit / spent_quote if spent_quote > 0 else 0.0
    effective_spread = (avg_sell_px - avg_buy_px) / avg_buy_px if avg_buy_px > 0 else 0.0

    return {
        "qty_base_bought": float(acquired_base),
        "qty_base_after_withdraw": float(transferable_base),
        "qty_base_sold": float(qty_sold),
        "spent_quote": float(spent_quote),
        "received_quote": float(received_quote),
        "buy_fee_quote": float(buy_fee_quote),
        "sell_fee_quote": float(sell_fee_quote),
        "withdraw_fee_base": float(withdraw_fee_base),
        "avg_buy_px": float(avg_buy_px),
        "avg_sell_px": float(avg_sell_px),
        "effective_spread": float(effective_spread),
        "net_profit_quote": float(net_profit),
        "roi": float(roi),
        "ok": 1 if (qty_sold > 0 and net_profit > 0) else 0
    }
