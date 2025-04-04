from  HedgeModel.vanilla.blackscholes import BSDigitalCall
from HedgeModel.calctype import CalcType
from HedgeModel.vanilla.vanilla import Vanilla
from datetime import date
import math


class DigitalCall(Vanilla):

    """ <Inherited>
    name: str = None
    # ntnl: float
    # contracts: float
    position_mult: float = 1.0
    idx_lvl_start: float
    idx: str
    strike: float
    exp_dt: date
    rate: Optional[float]
    """
    

    @property
    def payoff_per_contract(self) -> float:
        return self.idx_lvl_start * self.rate
        # return self.ntnl * self.rate / self.contracts
    
    
    def calc(self, s: float, r: float, q: float, vol: float, t: float, calctype: CalcType = CalcType.Price):
        
        price_dict = {}
                
        # Calc the result
        result_per_contract = self.position_mult * BSDigitalCall(s, self.strike, r, q, vol, t, self.payoff_per_contract, str(calctype))

        price_dict['name'] = self.__repr__()
        price_dict['calc'] = str(calctype)
        price_dict['s'] = s
        price_dict['k'] = self.strike
        price_dict['r'] = r
        price_dict['q'] = q
        price_dict['vol'] = vol
        price_dict['t'] = t
        # price_dict['result'] = result
        price_dict['result_per_contract'] = result_per_contract
        # price_dict['contracts'] = self.contracts
        # price_dict['result'] = result_per_contract * self.contracts

        return price_dict


    def attrib(self):
        pass

    def payoff(self, idx_lvl_exp_dt: float):
        return self.position_mult * self.payoff_per_contract if idx_lvl_exp_dt >= self.strike else 0.0