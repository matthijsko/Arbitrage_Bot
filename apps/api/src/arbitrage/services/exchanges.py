import ccxt
from functools import lru_cache

SUPPORTED = {"bitvavo": ccxt.bitvavo, "coinbase": ccxt.coinbase, "kraken": ccxt.kraken}

@lru_cache(maxsize=16)
def get_exchange(name: str):
    name = name.lower()
    if name not in SUPPORTED:
        raise ValueError(f"Exchange '{name}' not supported")
    klass = SUPPORTED[name]
    return klass({"enableRateLimit": True, "timeout": 15000})

def fetch_orderbook(name: str, symbol: str, limit: int = 50):
    ex = get_exchange(name)
    ob = ex.fetch_order_book(symbol, limit=limit)
    asks = [(float(p), float(a)) for p, a in ob.get("asks", []) if float(a) > 0]
    bids = [(float(p), float(a)) for p, a in ob.get("bids", []) if float(a) > 0]
    asks.sort(key=lambda x: x[0])
    bids.sort(key=lambda x: x[0], reverse=True)
    return asks, bids

def get_market_meta(name: str, symbol: str):
    ex = get_exchange(name)
    markets = ex.load_markets()
    m = markets[symbol]
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
