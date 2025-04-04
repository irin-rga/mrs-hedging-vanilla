from datetime import datetime, date
from pathlib import Path
import pandas as pd
import typing
import math
import os

from HedgeModel.MktData.equity_prices import EquityPrices
from HedgeModel.MktData.equity_vol import EquityVol

from HedgeModel.MktData.mktdatasvc import MktDataSvc

from crossasset.crossasset_base import CrossAssetContext
from nxpy import pro

pd.options.display.float_format = "{:,.2f}".format


class MktData(MktDataSvc):

    __default_tickers = ['MXEA Index', 'RTY Index', 'NDX Index', 'SPX Index', 'SPMARC5P Index']
    __conventions_path = Path(os.path.dirname(__file__), "USMktConventions.nxt")

    def __init__(self, tickers: typing.Optional[list] = None):
        
        self.tickers = self.__default_tickers if tickers is None else tickers        
        self.prices = EquityPrices()        
        self.vol_by_date = {}

        # self.app = pro.Application()
        # self.warning = pro.ApplicationWarning()

        # CrossAssetContext(self.app)
        self.cacontext = CrossAssetContext(pro.Application(), pro.ApplicationWarning())
        self.app = self.cacontext.get_app()
        self.warning = self.cacontext.get_warning()

        # Read calendars and conventions
        self.app.read_nxt(self.__conventions_path, self.warning)

    def load_implied_vol(self, asofdt: date):

        if asofdt in self.vol_by_date:
            return
        
        self.vol_by_date[asofdt] = {}   

        # print(f'Calling load_implied_vol of class MktData for {asofdt}')

        for ticker in self.tickers:

            print(f'Loading Vol+Yld+Dvd Data for {ticker} on {asofdt}...')
            
            if ticker == 'SPMARC5P Index':
                self.vol_by_date[asofdt][ticker] = None
            else:
                self.vol_by_date[asofdt][ticker] = EquityVol(self.app, asofdt, ticker, self.get_px(asofdt, ticker))

    
    # def get_px(self, asofdt: date):
    #     return self.prices.get_px(asofdt)
    
    def get_px(self, asofdt: date, ticker: str = None):
        return self.prices.get_px(asofdt=asofdt, idx=ticker)
    
    def get_equityvol_obj(self, asofdt: date, ticker: str):
                
        vols_on_asofdt = self.vol_by_date.get(asofdt, None)

        if vols_on_asofdt is not None:
            return vols_on_asofdt[ticker]
        else:
            print(f'No VolSurfaces Loaded for {asofdt}')
            return
        
    def get_iv(self, vs_date: date, ticker: str, expiry_dt: date, strike: float):

        if ticker == 'SPMARC5P Index':
            return 0.055
        
        eq_vol = self.get_equityvol_obj(vs_date, ticker)                
        iv = self.app.get_implied_volatility(eq_vol.vol_surf.get_id(), expiry_dt, strike, self.warning)
        return iv
    
    def get_df(self, vs_date: date, ticker: str, expiry_dt: date):

        if ticker == 'SPMARC5P Index':
            return 1.0

        eq_vol = self.get_equityvol_obj(vs_date, ticker)                
        df = self.app.get_discount_factor(eq_vol.yield_curve.get_id(), vs_date, expiry_dt, self.warning)
        return df
    
    def get_rfr(self, vs_date: date, ticker: str, expiry_dt: date):

        if ticker == 'SPMARC5P Index':
            return 0.0

        df = self.get_df(vs_date, ticker, expiry_dt)
        ttm = self.get_ttm(vs_date, expiry_dt)
        rfr = -math.log(df)/ttm
        return rfr
    
    def get_q(self, vs_date: date, ticker: str, expiry_dt: date):

        if ticker == 'SPMARC5P Index':
            return 0.0

        eq_vol = self.get_equityvol_obj(vs_date, ticker)
        yc_id = eq_vol.yield_curve.get_id()
        div_id = eq_vol.div_curve.get_id()
        basis = "Act/365.25"
        spot = eq_vol.spot_price
        q = self.app.get_dividend_yield(yc_id, div_id, vs_date, vs_date, expiry_dt, basis, spot, self.warning)
        return q
    
    def get_ttm(self, val_dt: date, expiry_dt: date, dayct_basis: typing.Optional[str] = None):

        basis = 'Act/365.25' if dayct_basis is None else dayct_basis
        return self.app.get_day_count_fraction(basis, val_dt, expiry_dt, self.warning)
    
    def third_friday(self, year, month):
        """Return datetime.date for monthly option expiration given year and
        month
        """
        # The 15th is the lowest third day in the month
        third = date(year, month, 15)
        # What day of the week is the 15th?
        w = third.weekday()
        # Friday is weekday 4
        if w != 4:
            # Replace just the day (of month)
            third = third.replace(day=(15 + (4 - w) % 7))
        return third
    
    def get_maturity(self, start_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'P', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):
        
        tnr_yrs = str(num_of_yrs) + 'Y'        
        return self.app.add_tenor(start_dt, tnr_yrs, conv, cal, self.warning, eom)

    #region 'Winterfell 1) Start\Effective from Expiry, and; 2) Expiry from Start\Effective Date Helper Functions
    
    def get_Winterfell_IUL_ExpiryDt(self, start_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

        """
        NOTE -- For Winterfell (IUL):
        Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
        Segment Maturity Date:  The 1st BD occuring after the 13th calendar day of the month
        """
        maturity_dt = start_dt + pd.DateOffset(years=num_of_yrs)
        maturity_dt = maturity_dt.replace(day=13)

        maturity_dt = self.app.add_tenor(maturity_dt, "1BD", conv, cal, self.warning, eom)
        return maturity_dt
    
    def get_Winterfell_IUL_StartDt_From_ExpiryDt(self, maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

        """
        NOTE -- For Winterfell (IUL):
        Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
        Segment Maturity Date:  The 1st BD occuring after the 13th calendar day of the month
        """
        start_dt = maturity_dt + pd.DateOffset(years=-1*num_of_yrs)
        start_dt = start_dt.replace(day=13)

        start_dt = self.app.add_tenor(start_dt, "2BD", conv, cal, self.warning, eom)
        return start_dt
    
    def get_Winterfell_VUL_ExpiryDt(self, start_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

        """
        NOTE -- For Winterfell (IUL):
        Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
        Segment Maturity Date:  The 1st BD occuring after the 13th calendar day of the month
        """        
        yr = start_dt.year + num_of_yrs
        mth = start_dt.month

        maturity_dt = self.third_friday(yr, mth)
        return maturity_dt        
    
    def get_Winterfell_VUL_StartDt_From_ExpiryDt(self, maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

        """
        NOTE -- For Winterfell (VUL):
        Segment Start Date: 3rd Friday
        Segment Maturity Date:  3rd Friday
        """
        
        yr = maturity_dt.year - num_of_yrs
        mth = maturity_dt.month
        
        start_dt = self.third_friday(yr, mth)
        return start_dt
    
    def get_Winterfell_StartDt_From_ExpiryDt(self, Product: str, maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

        if Product == 'IUL':
            return self.get_Winterfell_IUL_StartDt_From_ExpiryDt(maturity_dt, num_of_yrs, conv, cal, eom)
        else:
            return self.get_Winterfell_VUL_StartDt_From_ExpiryDt(maturity_dt, num_of_yrs, conv, cal, eom)
        
    
    def get_Winterfell_ExpiryDt(self, Product: str, maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

        if Product == 'IUL':
            return self.get_Winterfell_IUL_ExpiryDt(maturity_dt, num_of_yrs, conv, cal, eom)
        else:
            return self.get_Winterfell_VUL_ExpiryDt(maturity_dt, num_of_yrs, conv, cal, eom)

    
    #endregion 'Winterfell 1) Start\Effective from Expiry, and; 2) Expiry from Start\Effective Date Helper Functions



if __name__ == "__main__":

    tickers = ['MXEA Index', 'RTY Index', 'SPX Index']    
    mktdata = MktData(tickers)

    val_dt = date(2023, 1, 20)
    mktdata.load_implied_vol(val_dt)

    print()

