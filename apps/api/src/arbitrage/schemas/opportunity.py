from pydantic import BaseModel, Field
from datetime import datetime

class Opportunity(BaseModel):
    symbol: str = Field(..., examples=["BTC/USDT"])
    buy_exchange: str
    sell_exchange: str
    spread_bps: float = Field(..., description="Gross spread in basis points")
    net_profit_quote: float = Field(..., description="Net profit in quote currency for the given qty_base")
    qty_base: float
    ts: datetime
