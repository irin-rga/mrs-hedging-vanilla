from abc import ABC, abstractmethod
from dataclasses import dataclass
from HedgeModel.calctype import CalcType
from typing import Optional
from datetime import date

attrib_types = ['s','r','q','vol','t']

def get_attrib_params(bop_px_dict: dict, eop_px_dict: dict, attrib_type: str, calctype: CalcType):
        
    pd = {}
    key_idx = 999

    for i, key in enumerate(attrib_types):

        # Key Found.  Now it still goes to eop dictionary, but everything after goes to bop
        if attrib_type==key:
            key_idx = i

        # Once found, 
        if i > key_idx:
            pd[key] = bop_px_dict[key]
        else:
            pd[key] = eop_px_dict[key]

    # Add Calc type so we can unpack all these args as direct inputs to the 'calc' function
    pd['calctype'] = calctype
    
    # return price dictioanry
    return pd

@dataclass(frozen=True)
class Vanilla(ABC):
    
    # ntnl: float
    # contracts: float    
    idx_lvl_start: float
    idx: str
    strike: float
    exp_dt: date
    name: str = None
    position_mult: float = 1.0 # Enter -2.0 for double short, 3.0 for triple long
    rate: Optional[float] = None

    def __repr__(self):

        # side = 'Long' if self.contracts >=0 else 'Short'
        side = 'Long' if self.position_mult >=0 else 'Short'
        class_name = self.__class__.__name__

        default_descript = f"{side}_{round(self.strike, 2)}_Vanilla_{class_name}"
        descript = self.name if self.name else default_descript

        # return f"{side} {abs(self.contracts)} contracts of Vanilla {class_name}"
        return descript

    
    @abstractmethod
    def calc(self, s: float, r: float, q: float, vol: float, t: float, calctype: CalcType = CalcType.Price):
        pass

    @abstractmethod
    def payoff(self, idx_lvl_exp_dt: float):
        return 0.0

    @abstractmethod
    def attrib(self, price_dict_prev, price_dict_curr):
        # Use dictionary results of previous and current day full prices to simplify the attribution calcs!
        pass

    def shocked_calc(self, s: float, r: float, q: float, vol: float, t: float, calctype: CalcType = CalcType.Price, eq_shock: float = 0.0, rate_shock: float = 0.0, vol_shock: float = 0.0):
        """
        Apply a shock to the inputs and return the result.  This is useful for simple valuations
        Note:  Equity Shocks are applied as (1+eq_shock) * spot price, rate and vol shocks are additive
        """
        return self.calc(s*(1+eq_shock), r+rate_shock, q, vol+vol_shock, t, calctype)

    def get_attrib_params(self, bop_px_dict: dict, eop_px_dict: dict, attrib_type: str, calctype: CalcType):
        return get_attrib_params(bop_px_dict, eop_px_dict, attrib_type, calctype)
    
    
    





