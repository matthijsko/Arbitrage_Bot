from typing import TypedDict

class OpportunityLike(TypedDict):
    symbol: str
    buy_exchange: str
    sell_exchange: str
    spread_bps: float
    net_profit_quote: float
    qty_base: float
    ts: str
