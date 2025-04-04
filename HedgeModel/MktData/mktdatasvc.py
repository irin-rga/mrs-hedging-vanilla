from HedgeModel.MktData.equity_prices import EquityPrices
from HedgeModel.MktData.equity_vol import EquityVol
from typing import List, Dict, Optional
from abc import ABC, abstractmethod
from datetime import date


class MktDataSvc(ABC):

    tickers: List[str]
    prices: EquityPrices
    vol_by_date: Dict

    def __init__(self):
        pass

    @abstractmethod
    def load_implied_vol(self, asofdt: date):
        pass

    @abstractmethod        
    def get_px(self, asofdt: date, ticker: str = None):
        pass

    @abstractmethod        
    def get_iv(self, vs_date: date, ticker: str, expiry_dt: date, strike: float):
        pass

    @abstractmethod        
    def get_df(self, vs_date: date, ticker: str, expiry_dt: date):
        pass

    @abstractmethod        
    def get_rfr(self, vs_date: date, ticker: str, expiry_dt: date):
        pass

    @abstractmethod        
    def get_q(self, vs_date: date, ticker: str, expiry_dt: date):
        pass

    @abstractmethod        
    def get_ttm(self, val_dt: date, expiry_dt: date, dayct_basis: Optional[str] = None):
        pass