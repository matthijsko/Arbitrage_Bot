import ccxt
from functools import lru_cache
from .symbols import resolve_symbol_for_exchange

SUPPORTED = {
    "bitvavo": ccxt.bitvavo,
    "coinbase": ccxt.coinbase,  # Advanced Trade
    "kraken": ccxt.kraken,
}

def _sanitize_levels(levels):
    out = []
    for lvl in levels or []:
        try:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                price, amount = float(lvl[0]), float(lvl[1])
            elif isinstance(lvl, dict):
                price = float(lvl.get("price") or lvl.get("p") or lvl.get(0))
                amount = float(lvl.get("amount") or lvl.get("volume") or lvl.get("a") or lvl.get(1))
            else:
                continue
            if amount > 0:
                out.append((price, amount))
        except Exception:
            continue
    return out

@lru_cache(maxsize=16)
def get_exchange(name: str):
    name = name.lower()
    if name not in SUPPORTED:
        raise ValueError(f"Exchange '{name}' not supported")
    klass = SUPPORTED[name]
    return klass({"enableRateLimit": True, "timeout": 15000})

def fetch_orderbook(name: str, symbol: str, limit: int = 50):
    ex = get_exchange(name)
    sym = resolve_symbol_for_exchange(ex, symbol)
    ob = ex.fetch_order_book(sym, limit=limit)
    asks = _sanitize_levels(ob.get("asks"))
    bids = _sanitize_levels(ob.get("bids"))
    asks.sort(key=lambda x: x[0])
    bids.sort(key=lambda x: x[0], reverse=True)
    return asks, bids

def get_market_meta(name: str, symbol: str):
    ex = get_exchange(name)
    sym = resolve_symbol_for_exchange(ex, symbol)
    markets = ex.load_markets()
    m = markets[sym]
    taker = m.get("taker", ex.fees.get("trading", {}).get("taker", 0.001))
    precision = (m.get("precision") or {})
    limits = (m.get("limits") or {})
    base_step = precision.get("amount")
    min_base = (limits.get("amount") or {}).get("min")
    min_cost = (limits.get("cost") or {}).get("min")
    return {
        "taker_fee": float(taker if taker is not None else 0.001),
        "base_step": float(base_step) if base_step else None,
        "min_base": float(min_base) if min_base else None,
        "min_notional": float(min_cost) if min_cost else None,
    }
