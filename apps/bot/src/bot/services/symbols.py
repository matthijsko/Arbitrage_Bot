from typing import Optional, Dict, Any, Iterable

BTC_SYNONYMS = {"BTC", "XBT"}

def _norm(x: Optional[str]) -> str:
    return (x or "").upper().strip()

def _base_candidates(base: str) -> Iterable[str]:
    b = _norm(base)
    if b in BTC_SYNONYMS:
        return BTC_SYNONYMS
    return {b}

def resolve_symbol_for_exchange(ex, canonical_symbol: str) -> str:
    if "/" not in canonical_symbol:
        raise ValueError(f"Invalid symbol '{canonical_symbol}', expected BASE/QUOTE")
    base, quote = canonical_symbol.split("/", 1)
    bset = { _norm(x) for x in _base_candidates(base) }
    q = _norm(quote)

    markets: Dict[str, Dict[str, Any]] = ex.load_markets()
    if canonical_symbol in markets:
        return canonical_symbol
    for m in markets.values():
        if not m or not m.get("symbol"):
            continue
        if not m.get("active", True):
            continue
        if _norm(m.get("quote")) == q and _norm(m.get("base")) in bset:
            return m["symbol"]
    raise ValueError(f"Symbol '{canonical_symbol}' not found for exchange '{getattr(ex, 'id', '?')}'")
