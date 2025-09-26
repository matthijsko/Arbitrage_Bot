import ccxt
from functools import lru_cache
from typing import Dict, List, Optional, Tuple, Any

SUPPORTED = {
    "bitvavo": ccxt.bitvavo,
    "coinbase": ccxt.coinbase,  # Advanced Trade spot
    "kraken": ccxt.kraken,
}

@lru_cache(maxsize=16)
def get_exchange(name: str):
    name = name.lower()
    if name not in SUPPORTED:
        raise ValueError(f"Exchange '{name}' not supported")
    klass = SUPPORTED[name]
    return klass({"enableRateLimit": True, "timeout": 20000})

def load_markets(name: str) -> Dict[str, Dict[str, Any]]:
    ex = get_exchange(name)
    return ex.load_markets()

def list_symbols(name: str) -> List[str]:
    markets = load_markets(name)
    return sorted([m for m in markets.keys() if markets[m].get("active", True)])

def list_symbols_with_quote(name: str, quote: str) -> List[str]:
    quote = quote.upper()
    out = []
    markets = load_markets(name)
    for sym, m in markets.items():
        if not m or not m.get("symbol"):
            continue
        if m.get("quote", "").upper() == quote and m.get("active", True):
            out.append(sym)
    return sorted(out)

def fetch_orderbook(name: str, symbol: str, limit: int = 50) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    ex = get_exchange(name)
    ob = ex.fetch_order_book(symbol, limit=limit)
    asks = [(float(p), float(a)) for p, a in ob.get("asks", []) if float(a) > 0]
    bids = [(float(p), float(a)) for p, a in ob.get("bids", []) if float(a) > 0]
    asks.sort(key=lambda x: x[0])
    bids.sort(key=lambda x: x[0], reverse=True)
    return asks, bids

def fetch_ticker(name: str, symbol: str) -> Dict[str, Any]:
    ex = get_exchange(name)
    return ex.fetch_ticker(symbol)

def get_market_meta(name: str, symbol: str) -> Dict[str, Optional[float]]:
    ex = get_exchange(name)
    markets = ex.load_markets()
    m = markets[symbol]

    # taker/maker fees
    taker = m.get("taker", ex.fees.get("trading", {}).get("taker"))
    maker = m.get("maker", ex.fees.get("trading", {}).get("maker"))

    precision = (m.get("precision") or {})
    limits = (m.get("limits") or {})

    base_step = precision.get("amount")
    price_step = precision.get("price")

    min_base = (limits.get("amount") or {}).get("min")
    max_base = (limits.get("amount") or {}).get("max")
    min_cost = (limits.get("cost") or {}).get("min")
    max_cost = (limits.get("cost") or {}).get("max")

    # (optioneel) withdraw fee als ccxt ‘m expose’t (niet elke venue)
    withdraws = ex.fees.get("funding", {}).get("withdraw", {}) if getattr(ex, "fees", None) else {}
    base = (m.get("base") or "").upper()
    withdraw_fee_base = None
    if isinstance(withdraws, dict) and base in withdraws:
        fee_val = withdraws[base]
        if isinstance(fee_val, dict):
            withdraw_fee_base = fee_val.get("fee")
        elif isinstance(fee_val, (int, float)):
            withdraw_fee_base = float(fee_val)

    return {
        "taker_fee": float(taker) if taker is not None else None,
        "maker_fee": float(maker) if maker is not None else None,
        "base_step": float(base_step) if base_step else None,
        "price_step": float(price_step) if price_step else None,
        "min_base": float(min_base) if min_base else None,
        "max_base": float(max_base) if max_base else None,
        "min_notional": float(min_cost) if min_cost else None,
        "max_notional": float(max_cost) if max_cost else None,
        "withdraw_fee_base": float(withdraw_fee_base) if withdraw_fee_base is not None else None,
        "base": m.get("base"),
        "quote": m.get("quote"),
        "active": bool(m.get("active", True)),
    }

def ping(name: str) -> Dict[str, Any]:
    # Simpele ping door time/fetch of load_markets; ccxt heeft geen uniforme ping
    ex = get_exchange(name)
    try:
        t = ex.fetch_time()
    except Exception:
        t = None
    try:
        ms = ex.milliseconds()
    except Exception:
        ms = None
    ok = True
    if t is None and ms is None:
        ok = False
    return {"ok": ok, "server_time": t, "local_ms": ms}
