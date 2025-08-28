from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class OHLCVRow(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    adj_close: Optional[float] = None
    volume: float

class TickerDataOut(BaseModel):
    ticker: str
    ohlcv: List[OHLCVRow] = []
    info: Dict[str, Any] = {}
    error: Optional[str] = None

class TechnicalsOut(BaseModel):
    ticker: str
    latest: Dict[str, Any]
    macd_diff: Optional[float] = None
    sma_cross: Optional[str] = None
    bollinger_band_width: Optional[float] = None
    error: Optional[str] = None

class FundamentalsOut(BaseModel):
    source: str
    eps: Optional[float] = None
    revenue: Optional[float] = None
    net_income: Optional[float] = None
    ebitda: Optional[float] = None
    pe_ratio: Optional[float] = None
    price_to_sales: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    free_cash_flow_yield: Optional[float] = None
    peg_ratio: Optional[float] = None
    insider_transactions_count_90d: Optional[int] = None
    forward_guidance: Optional[str] = None
    error: Optional[str] = None

class OptionContract(BaseModel):
    expiry: str
    type: str  # "call" | "put"
    strike: float
    last: Optional[float]
    volume: Optional[int]
    openInterest: Optional[int]
    iv: Optional[float]
    delta: Optional[float]
    pop_itm: Optional[float]

class OptionsDataOut(BaseModel):
    ticker: str
    underlying_price: Optional[float] = None
    options: List[OptionContract] = []
    error: Optional[str] = None

class OptionCandidate(BaseModel):
    type: str
    strike: float
    pop_itm: Optional[float]
    credit: float
    max_loss: float
    credit_to_max_loss: float
    meets_rules: bool
    volume: Optional[int]
    openInterest: Optional[int]
    expiry: str

class OptionsScreenOut(BaseModel):
    ticker: str
    candidates: List[OptionCandidate] = []
    error: Optional[str] = None

class SmartMoneyOut(BaseModel):
    ticker: str
    insider_90d_net_buy: Optional[int] = None
    institutional_holders: Optional[list] = None
    congress_trades_30d: Optional[list] = None
    error: Optional[str] = None

class FinalSignalOut(BaseModel):
    signal: str = Field(..., pattern="^(Buy|Sell|Hold)$")
    confidence: int = Field(..., ge=0, le=100)
    reason: str
