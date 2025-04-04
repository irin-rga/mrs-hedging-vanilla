from  HedgeModel.vanilla.blackscholes import BSCall
from HedgeModel.calctype import CalcType
from HedgeModel.vanilla.vanilla import Vanilla
from datetime import date
import math


class Call(Vanilla):

    """ <Inherited>
    name: str = None
    # ntnl: float
    # contracts: float
    position_mult: float = 1.0
    idx_lvl_start: float
    idx: str
    strike: float
    exp_dt: date
    """
    
    
    def calc(self, s: float, r: float, q: float, vol: float, t: float, calctype: CalcType = CalcType.Price):
        
        price_dict = {}
        
        # Calc the result
        result_per_contract = self.position_mult * BSCall(s, self.strike, r, q, vol, t, str(calctype))

        price_dict['name'] = self.__repr__()
        price_dict['calc'] = str(calctype)
        price_dict['s'] = s
        price_dict['k'] = self.strike
        price_dict['r'] = r
        price_dict['q'] = q
        price_dict['vol'] = vol
        price_dict['t'] = t
        price_dict['result_per_contract'] = result_per_contract
        # price_dict['contracts'] = self.contracts
        # price_dict['result'] = result_per_contract * self.contracts

        return price_dict
    
    def payoff(self, idx_lvl_exp_dt: float):
        return self.position_mult * max(0.0, idx_lvl_exp_dt - self.strike)


    def attrib(self):
        pass