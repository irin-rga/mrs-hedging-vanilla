from HedgeModel.calctype import CalcType
from HedgeModel.ntnl_attrib import NtnlAttrib, PlcyChgAttrib, MktChgAttrib, FullAttrib
from HedgeModel.optioncombofactory import create_option_combo
from HedgeModel.MktData.mktdatasvc import MktDataSvc
from HedgeModel.valuation_shocks import ValuationShocks
from utils.decoration_utils import timing, timer
from HedgeModel.MktData.mkt_data import MktData
from HedgeModel.optioncombo import OptionCombo

from HedgeModel.opttype import OptType
from HedgeModel import optioncombo


from typing import Dict, Any, List, Tuple, Union, Optional
from dataclasses import dataclass, fields, field
from datetime import date
import sys



req_flds = ['Opt_Type', 'Bbg_Idx', 'IdxLvl_StartDt', 'Strike_Low', 'Strike_High', 'ExpiryDt']
opt_flds = ['ID', 'Ref_ID', 'Group_ID', 'Ntnl_Attrib', 'Ntnl', 'Contracts', 'Rate', 'Buffer', 'TradePrice_Entry', 'TradePrice_Exit']
ntl_flds = ['Ntnl_BoP', 'Ntnl_Added', 'Ntnl_Chg', 'Ntnl_Decr', 'Ntnl_Matured', 'Ntnl_EoP']

@dataclass(frozen=True)
class Position:
            
    # Opt_Type: str # Used to Set OptType enum
    Opt_Type: OptType
    # OptionType: OptType = field(init=True)
    Options: OptionCombo = field(init=False)
    
    # Opt_Type: str
    Bbg_Idx: str # Used to get market prices and vol when needed
    IdxLvl_StartDt: float

    # For Simple Calls/Puts, set single strike to Strike_Low
    Strike_Low: float
    # Strike_High: float

    # Date Related Fields
    ExpiryDt: date

    # Default Fieldds
    ID: int = None # Unique ID created/used by RGA for internal purposes
    Ref_ID: object = None # External/Client unique reference id -- for recon vs. client reports
    Group_ID: object = None # ID used to group individual vanilla options back into 'combinations' of options <such as a long ATM and short OTM calls into a call spread
    
    # Should be FINAL <Adjusted> Notional.  Should reflect the full liability or asset.  Hedge Ratios should NOT be applied.  Should be Net of Coins.  Any participation or notionals should already be reflected in this amount!
    Ntnl: float = None
    Contracts: float = None
    Rate: float = None
    Buffer: float = 1.0

    # Used for Attribution
    Ntnl_Attrib: NtnlAttrib = field(default=None, init=True)

    # Made optional since it doesn't matter for non spread like options
    Strike_High: Optional[float] = None

    TradePrice_Entry: Optional[float] = None
    TradePrice_Exit: Optional[float] = None

    # def __init__(self, opt_type: str, idx: str, idx_lvl_start: float, k_low: float, k_high: float, exp: date, **kwargs):
    def __init__(self, **kwargs: Any):
        # Initialize only the defined fields
        
        for key, value in kwargs.items():
            # print(f'Key:Value is {key}:{value}')
            if key in req_flds:
                # print(f'{key} is in required fields, so setting to {value}')
                object.__setattr__(self, key, value)                
            elif key in opt_flds:
                # print(f'{key} is in optional fields, so setting to {value}')
                object.__setattr__(self, key, value)
            # else:
                # print(f'{key} was not found.')

        if any(k in ntl_flds for k in kwargs.keys()):
            ntnl_dict = {k:v for k, v in kwargs.items() if k in ntl_flds}
            temp_ntnl_attrib = NtnlAttrib(**ntnl_dict)
            object.__setattr__(self, 'Ntnl_Attrib', temp_ntnl_attrib)
            # self.Ntnl_Attrib = NtnlAttrib(**ntnl_dict)
        else:        
            if self.Ntnl is None or self.Contracts is None:
                print('Error!  Either Ntnl and Contracts must be provided, or a NtnlAttrib object must be provided!')
            else:
                ttl_ntnl = self.Ntnl * self.Contracts
                temp_ntnl_attrib = NtnlAttrib({'Ntnl_BoP': ttl_ntnl, 'Ntnl_EoP': ttl_ntnl})
                object.__setattr__(self, 'Ntnl_Attrib', temp_ntnl_attrib)

        # Create the list of options representing this position
        opt_list = create_option_combo(self.Opt_Type, self.Bbg_Idx, self.IdxLvl_StartDt, self.Strike_Low, self.Strike_High, self.ExpiryDt, self.Buffer, self.Rate)
        opt_combo = OptionCombo(opt_list, name=str(self.Opt_Type))
        object.__setattr__(self, 'Options', opt_combo)



    def price_shocks(self, mds: MktDataSvc, val_dt: date, shocks: List[ValuationShocks]):
        """
        Apply a shock to the inputs and return the result.  This is useful for simple valuations
        Note:  Equity Shocks are applied as (1+eq_shock) * spot price, rate and vol shocks are additive
        """

        # Initialize Results Object
        ntnl_in_contracts = self.Ntnl_Attrib.convert_to_contracts(self.IdxLvl_StartDt)
        num_of_contracts = ntnl_in_contracts.Ntnl_BoP # Can extract from either BoP or EoP since they should be the same for a single valuation date
        base_price = None
        results_dict = {}
        
        for shock in shocks:

            # Extract Shock Amts from Shock Record
            equity_shock = shock.ShockAmt if shock.ShockType.upper() == 'EQUITY' else 0.0
            rate_shock = shock.ShockAmt if shock.ShockType.upper() == 'RATE' else 0.0
            vol_shock = shock.ShockAmt if shock.ShockType.upper() == 'VOL' else 0.0

            # Get Price Dictionary of Result
            results_dict = self.Options.shocked_calc(mds, val_dt, calctype=CalcType.Price, equity_shock=equity_shock, rate_shock=rate_shock, vol_shock=vol_shock)
            price_per_contract = results_dict[self.Options.total_tag()]
            total_price = price_per_contract * num_of_contracts
                                                
            # Add price result to the dictionary 
            results_dict[shock.price_field] = total_price

            if shock.ShockName.upper() == 'BASE':
                # Update base_price in order to facilitate the calculation of the impact of the other shocks
                base_price = total_price
            else:            
                results_dict[shock.impact_field] = total_price - base_price

        # Return the dictionary of results
        return results_dict



    # region Attribution Functions

    # region Ntnl Attribution Helper Functions
    @property 
    def ntnl_attrib_needed(self):
        if self.Ntnl_Attrib is None:
            return False
        else:
            return self.Ntnl_Attrib.attrib_needed
        
    @property
    def added_ntnl_needs_attrib(self):
        if self.Ntnl_Attrib:
            if self.Ntnl_Attrib.Ntnl_Added != 0.0:
                return self.Ntnl_Attrib.Ntnl_BoP != 0.0
        return True

        
    def calc_ntnl_attrib(self, mds: MktDataSvc, start_dt: date, end_dt: date = None):
        
        """
        Calculate All Non-Mkt Related Changes in MV <Due to Changes in Policyholder Ntnl>.  
        NOTE:  Includes Attribution for Maturing Positions!
        """
        # if self.ntnl_attrib_needed or (self.ExpiryDt == end_dt):
        if self.ntnl_attrib_needed:

            # Convert Ntnl in $$$ to Equivalent # of contracts (since price results are per contract) and then set the starting MV (BoP)        
            ntnl_in_contracts = self.Ntnl_Attrib.convert_to_contracts(self.IdxLvl_StartDt)
            contracts_bop = ntnl_in_contracts.Ntnl_BoP

            # Calc and Set MV_BoP <Note: this will still be zero for added/new contracts since BoP will be zero!>
            price_bop_dict = self.Options.calc(mds, start_dt)
            price_per_contract_bop = price_bop_dict[self.Options.total_tag()]
            # mv_bop = price_bop_dict[self.Options.total_tag()] * ntnl_in_contracts.Ntnl_BoP

            # IF MATURED <Matured is special case where payoff and chg at maturity needed.  The non maturing case is the simplest!
            if self.ExpiryDt == end_dt:
                return self.calc_attrib_maturity(mds, price_per_contract_bop, contracts_bop)
            else:                
                attrib_added = self.calc_attrib_added(mds, ntnl_in_contracts, price_per_contract_bop, end_dt)
                return self.calc_ntnl_attrib_non_maturing(ntnl_in_contracts, price_per_contract_bop, attrib_added)       

        else:            
            # Return the empty <defaulted to zero> Attribution Object
            return FullAttrib()
                
    def calc_attrib_maturity(self, mds: MktDataSvc, price_per_contract_bop: float, contracts_bop: float):
                
        # TODO:  Add Logic for when position field 'TradePrice_Exit ' is provided (which would take the place of the end of day market close price when calculating attribution)
        
        # Calc Payoff
        payoff_per_contract = None

        if self.TradePrice_Exit:
            payoff_per_contract = self.TradePrice_Exit
        else:
            mat_px = mds.get_px(self.ExpiryDt, self.Bbg_Idx)
            payoff_dict = self.Options.payoff(mat_px)
            payoff_per_contract = payoff_dict[self.Options.total_tag('PayOff')]

        ttl_payoff = payoff_per_contract * contracts_bop

        # Set the Chg for Matured equal to the last day of MV movement (payoff - prev_day).  Set Chg_in_MV for Payoff = -Payoff, to make the final equal to the sum of parts
        full_attrib = FullAttrib()
        full_attrib.MV_BoP = price_per_contract_bop * contracts_bop
        full_attrib.MV_Chg_Ntnl_Matured = ttl_payoff - full_attrib.MV_BoP
        full_attrib.MV_Chg_PayOff = -ttl_payoff
        # full_attrib.MV_Chg_PayOff = ttl_payoff - full_attrib.MV_BoP

        # We can return the attribution for this record, since we only need to set MV_BoP + MV_Chg_Ntnl_Matured + MV_Chg_PayOff = 0.0 == MV_EoP
        return full_attrib
    
    def calc_attrib_added(self, mds: MktDataSvc, ntnl_in_contracts: NtnlAttrib, price_per_contract_bop: float, end_dt: date):
                
        full_attrib = FullAttrib()
                        
        if ntnl_in_contracts.Ntnl_Added != 0.0:

            # Always calc price at EoP <in order to get MV_Chg_Ntnl_Added>
            price_eop_dict = self.Options.calc(mds, end_dt)
            price_per_contract_eop = price_eop_dict[self.Options.total_tag()]

            # extract #of contracts at BoP and added
            contracts_bop = ntnl_in_contracts.Ntnl_BoP
            contracts_added = ntnl_in_contracts.Ntnl_Added
            
            # For new price, TradePrice_Entry if available, else use BoP if there was one, otherwise use EoP            
            initial_price_per_contract = self.TradePrice_Entry if self.TradePrice_Entry else (price_per_contract_bop if contracts_bop != 0.0 else price_per_contract_eop)

            # For new, its # of added contracts * initial price per contract
            full_attrib.MV_Chg_New = contracts_added * initial_price_per_contract

            # Calc chg in price per contract
            chg_in_price_per_contract = price_per_contract_eop - initial_price_per_contract

            # Calculate change in MV for Newly Added Contracts, But only if there were no contracts at BoP.  If there were contracts at BoP, then the change in MV for added is zero and the changes to EoD will be captured by mkt attrib
            full_attrib.MV_Chg_Ntnl_Added = (contracts_added * chg_in_price_per_contract) if contracts_bop == 0.0 else 0.0            

            # Add MV_EoP for case when BoP contracts = 0.0, since this won't go through the mkt attrib process
            full_attrib.MV_EoP = (contracts_added * price_per_contract_eop) if contracts_bop == 0.0 else 0.0

        # Return the result
        return full_attrib
            
                
    def calc_ntnl_attrib_non_maturing(self, ntnl_in_contracts: NtnlAttrib, price_per_contract_bop: float, full_attrib: FullAttrib):
        """
        Simply calculate the Impact on MV Changes due to PolicyHolder Ntnl Changes by Assessing Ntnl Chgs in Terms of Chgs in #of Contracts (and then multiplying by BoP Contract Price).
        This is equivalent to saying all PolicyHolder Behavior <Ntnl Chgs> occure after the market closes on the attribution start date, but before the market opens on the attribution end date>
        """
                
        full_attrib.MV_BoP = price_per_contract_bop * ntnl_in_contracts.Ntnl_BoP        
        full_attrib.MV_Chg_Ntnl_Chg = price_per_contract_bop * ntnl_in_contracts.Ntnl_Chg
        full_attrib.MV_Chg_Ntnl_Decr = price_per_contract_bop * ntnl_in_contracts.Ntnl_Decr

        # Return result
        return full_attrib

    # endregion Ntnl Attribution Helper Functions

                
    # @timer
    def calc_attrib(self, mds: MktDataSvc, start_dt: date, end_dt: date, debug_mode=False):
        
        # Return a results object populated w/ the impact of ntnl changes or with default zeros
        full_attrib = self.calc_ntnl_attrib(mds, start_dt, end_dt)

        if (self.ExpiryDt == end_dt) or (not self.added_ntnl_needs_attrib):
            # No need to roll fwd mkt on matured position!
            return full_attrib.to_dict()
        
        # Now calculate the impact of Chgs in MV due to changes in market params (BS RollFwd)
        price_bop_dict = self.Options.calc(mds, start_dt)                        
        price_eop_dict = self.Options.calc(mds, end_dt)
        mkt_attrib_dict = self.Options.calc_mkt_attrib_from_price_dicts(price_bop_dict, price_eop_dict, debug_mode)

        if debug_mode:
            return mkt_attrib_dict

        # Get Ending #of Contracts
        ntnl_in_contracts = self.Ntnl_Attrib.convert_to_contracts(self.IdxLvl_StartDt)        
        contracts_bop = ntnl_in_contracts.Ntnl_BoP
        contracts_eop = ntnl_in_contracts.Ntnl_EoP

        # Make sure we have the start and end captured
        full_attrib.MV_BoP = mkt_attrib_dict['BoP'] * contracts_bop
        full_attrib.MV_EoP = mkt_attrib_dict['EoP'] * contracts_eop

        """ The following 2 calcs should be equal as a check
        # Need to get the starting point of the mkt attriubtion (the MV_BoP after ntnl chgs)
        mv_bop_after_ntnl_chgs = full_attrib.mv_bop_after_ntnl_chgs
        # NOTE:  This should be the same as the BoP Price * EoP Contracts!
        mv_bop_after_ntnl_chgs_v2 = mkt_attrib_dict['BoP'] * contracts_eop
        """

        full_attrib.update_mkt_attrib(MktChgAttrib.calc_mv_chgs_from_mv_attrib_dict_and_contracts(mkt_attrib_dict, contracts_eop))

        # Return the dictionary of results
        return full_attrib.to_dict()
    
    # endregion Attribution Functions