from typing import List, Tuple, Union
from HedgeModel.calctype import CalcType
from HedgeModel.vanilla.vanilla import Vanilla, attrib_types
from HedgeModel.MktData.mktdatasvc import MktDataSvc
from HedgeModel.ntnl_attrib import NtnlAttrib
from datetime import date


class OptionCombo():


    def __init__(self, options: List['Vanilla'], name: str = None):
        self.options = options
        self.name = name
    
    
    """ Original Idea
    def calc(self, s: float, r: float, q: float, vol: float, t: float, calctype: CalcType = CalcType.Price):
        
        price_dict = {opt.__repr__(): opt.calc(s, r, q, vol, t, calctype) for opt in self.options}

        #NOTE: Goal is for other single vanilla options to return 
        
        # for i, option in enumerate(self.options):        
        # for option in self.options:                        
        #     price_dict[(asofdt, scen)][option.__repr__()] = option.price(asofdt, idx_price, calctype, scen)
    """

    def payoff(self, idx_lvl_exp_dt: float):

        # Initialize Results Object and ttl        
        payoff_dict = {}
        ttl = 0.0
        
        # Call 'payoff' for each option and track the total
        for opt in self.options:
            payoff = opt.payoff(idx_lvl_exp_dt)
            payoff_dict[opt.__repr__()] = payoff
            ttl += payoff

        # Add total to results dictionary
        payoff_dict[self.total_tag('PayOff')] = ttl

        # return dictionary
        return payoff_dict
        
    def calc(self, mktdatasvc: MktDataSvc, val_dt: date, calctype: CalcType = CalcType.Price): #, mkt_scen: str = 'Base'):

        # Make Sure Markit Vol Surf Data has been loaded!
        # mktdatasvc.load_implied_vol(val_dt)

        # Initialize Results Object and ttl
        price_dict = {}
        ttl = 0.0        

        # Call 'calc' for each option
        for opt in self.options:

            s = mktdatasvc.get_px(val_dt, opt.idx)
            r = mktdatasvc.get_rfr(val_dt, opt.idx, opt.exp_dt)
            q = mktdatasvc.get_q(val_dt, opt.idx, opt.exp_dt)
            vol = mktdatasvc.get_iv(val_dt, opt.idx, opt.exp_dt, opt.strike)
            t = mktdatasvc.get_ttm(val_dt, opt.exp_dt)

            # NOTE:  The following can be used to view yield curve
            # print(mktdatasvc.get_equityvol_obj(date(2025, 1, 2), opt.idx).yield_curve.view_data())
            # mktdatasvc.app.get_discount_factor(mktdatasvc.get_equityvol_obj(date(2025, 1, 2), opt.idx).yield_curve.get_id(), date(2025, 1, 2), date(2025, 1, 31), mktdatasvc.warning)

            # Include mkt_scen in Dictionary?
            result_dict = opt.calc(s, r, q, vol, t, calctype)
            price_dict[opt.__repr__()] = result_dict
            ttl += result_dict['result_per_contract']

        # Add total to results dictionary
        price_dict[self.total_tag(str(calctype))] = ttl

        # return dictionary
        return price_dict
    

    def shocked_calc(self, mktdatasvc: MktDataSvc, val_dt: date, calctype: CalcType = CalcType.Price, equity_shock: float = 0.0, rate_shock: float = 0.0, vol_shock: float = 0.0): 

        # Initialize Results Object and ttl
        price_dict = {}
        ttl = 0.0

        for opt in self.options:

            s = mktdatasvc.get_px(val_dt, opt.idx)
            r = mktdatasvc.get_rfr(val_dt, opt.idx, opt.exp_dt)
            q = mktdatasvc.get_q(val_dt, opt.idx, opt.exp_dt)
            vol = mktdatasvc.get_iv(val_dt, opt.idx, opt.exp_dt, opt.strike)
            t = mktdatasvc.get_ttm(val_dt, opt.exp_dt)

            # Include mkt_scen in Dictionary?
            result_dict = opt.shocked_calc(s, r, q, vol, t, calctype, equity_shock, rate_shock, vol_shock)
            price_dict[opt.__repr__()] = result_dict
            ttl += result_dict['result_per_contract']

        # Add total to results dictionary
        price_dict[self.total_tag(str(calctype))] = ttl

        # return dictionary
        return price_dict


    # May want to send prev_dt and curr_dt 1st!
    # def attrib(self, price_dict_prev, price_dict_curr):


    def calc_ntl_chg_attrib(self, price_bop: dict, ntnl_chgs: NtnlAttrib):
        pass
        # Get the Ntnl Chgs in Terms of Contracts

    def calc_mkt_attrib_from_price_dicts(self, price_bop: dict, price_eop: dict, debug_mode = False, calctype: CalcType = CalcType.Price, ):        

        price_attrib = self._attrib_mkt_chgs_from_price_dicts(price_bop, price_eop, calctype)
        attrib_summary = self.summarize_mkt_chg_attrib_results(price_bop, price_eop, price_attrib, debug_mode)

        return attrib_summary

    def calc_mkt_attrib_from_mktsvc(self, mktdatasvc: MktDataSvc, start_dt, end_dt, debug_mode = False, calctype: CalcType = CalcType.Price):    

        price_bop = self.calc(mktdatasvc, start_dt)
        price_eop = self.calc(mktdatasvc, end_dt)
        price_attrib = self._attrib_mkt_chgs_from_price_dicts(price_bop, price_eop, calctype)
        attrib_summary = self.summarize_mkt_chg_attrib_results(price_bop, price_eop, price_attrib, debug_mode)

        return attrib_summary
    
        
    def _attrib_mkt_chgs_from_price_dicts(self, price_bop: dict, price_eop: dict, calctype: CalcType = CalcType.Price):

        # Initialize an Empty Dictionary
        model_px_chg_attrib = {}

        for attrib_type in attrib_types:
            
            attrib_tag = f'Chg_in_{attrib_type}'
            model_px_chg_attrib[attrib_tag] = {}
            ttl = 0.0

            for opt in self.options:
                bop_results = price_bop[opt.__repr__()]
                eop_results = price_eop[opt.__repr__()]
                result_calc = opt.calc(*opt.get_attrib_params(bop_results, eop_results, attrib_type, calctype).values())
                ttl += result_calc['result_per_contract']
                model_px_chg_attrib[attrib_tag][opt.__repr__()] = result_calc

            # Now add the total to the results            
            model_px_chg_attrib[attrib_tag][self.total_tag(str(calctype))] = ttl

        return model_px_chg_attrib

    def summarize_mkt_chg_attrib_results(self, price_bop:dict, price_eop:dict, price_attrib:dict, debug_mode: bool = False):

        full_attrib_dict = {}

        if debug_mode:
            # Return everything                            
            full_attrib_dict['BoP'] = price_bop
            full_attrib_dict['EoP'] = price_eop
            full_attrib_dict = full_attrib_dict | price_attrib
        else:
            # Return totals only
            ttl_tag = self.total_tag()
            full_attrib_dict['BoP'] = price_bop[ttl_tag]
            full_attrib_dict['EoP'] = price_eop[ttl_tag]
            # attrib_dict_totals_only = {k: {k1: v1 for k1, v1 in v.items() if k1==ttl_tag} for k, v in price_attrib.items()}            
            attrib_dict_totals_only = {k: v1 for k, v in price_attrib.items() for k1, v1 in v.items() if k1==ttl_tag}            
            full_attrib_dict = full_attrib_dict | attrib_dict_totals_only

        return full_attrib_dict


    def total_tag(self, calctype: str = str(CalcType.Price)):
        # Now add the total to the results
        return f'Total_{str(calctype)}_Per_Contract'
    

        


