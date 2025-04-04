from dataclasses import dataclass, fields
from decimal import DivisionByZero

@dataclass(frozen=True)
class NtnlAttrib:
    
    Ntnl_BoP : float = 0.0
    Ntnl_Added: float = 0.0
    Ntnl_Chg: float = 0.0
    Ntnl_Decr: float = 0.0
    Ntnl_Matured: float = 0.0
    Ntnl_EoP : float = 0.0

    @classmethod
    def field_names(cls):
        return [f.name for f in fields(cls)]

    @property
    def attrib_needed(self) -> bool:
        return False if (self.Ntnl_BoP == self.Ntnl_EoP) else True        
    
    @property
    def new_or_added_ntnl(self) -> bool:
        if (self.Ntnl_BoP == 0.0) or (self.Ntnl_Added != 0.0):
            return True
        else:
            return False
                
    def convert_to_contracts(self, idx_lvl_start: float):        
        # Convert the Ntnl Attribution from Ntnl to #of Contracts
        if idx_lvl_start == 0.0:
            raise DivisionByZero

        return NtnlAttrib(**{f.name: getattr(self,f.name)/idx_lvl_start for f in fields(self)})
        
            
@dataclass
class PlcyChgAttrib:

    MV_Chg_New: float = 0.0
    MV_Chg_Ntnl_Added: float = 0.0
    MV_Chg_Ntnl_Chg: float = 0.0
    MV_Chg_Ntnl_Decr: float = 0.0
    MV_Chg_Ntnl_Matured: float = 0.0
    MV_Chg_PayOff: float = 0.0

@dataclass
class MktChgAttrib:

    # MV_BoP: float = 0.0
    MV_Chg_Spot: float = 0.0
    MV_Chg_RFR: float = 0.0
    MV_Chg_Dvd: float = 0.0
    MV_Chg_Vol: float = 0.0
    MV_Chg_Time: float = 0.0
    # MV_EoP: float = 0.0

    @staticmethod
    def calc_mv_chgs_from_mv_attrib_dict_and_contracts(mkt_attrib_dict: dict, contracts_eop) -> "MktChgAttrib":

        # Calc MVs at start, after each chg in paramater, and end
        mv_bop = mkt_attrib_dict['BoP'] * contracts_eop 
        mv_spot = mkt_attrib_dict['Chg_in_s'] * contracts_eop
        mv_rfr = mkt_attrib_dict['Chg_in_r'] * contracts_eop
        mv_dvd = mkt_attrib_dict['Chg_in_q'] * contracts_eop
        mv_vol = mkt_attrib_dict['Chg_in_vol'] * contracts_eop
        mv_t = mkt_attrib_dict['Chg_in_t'] * contracts_eop
        # mv_eop = mkt_attrib_dict['EoP'] * contracts_eop

        # Note:  Could add a check that mv_eop == mv_t

        # Set Attrib MV_Chg items based on above diffs
        return MktChgAttrib(
            # MV_BoP = mv_bop,
            MV_Chg_Spot = mv_spot - mv_bop,
            MV_Chg_RFR = mv_rfr - mv_spot,
            MV_Chg_Dvd = mv_dvd - mv_rfr,
            MV_Chg_Vol = mv_vol - mv_dvd,
            MV_Chg_Time = mv_t - mv_vol,
            # MV_EoP = mv_eop)
        )


        


@dataclass
class FullAttrib:

    MV_BoP: float = 0.0
    MV_Chg_New: float = 0.0
    MV_Chg_Ntnl_Added: float = 0.0
    MV_Chg_Ntnl_Chg: float = 0.0
    MV_Chg_Ntnl_Decr: float = 0.0
    MV_Chg_Ntnl_Matured: float = 0.0
    MV_Chg_PayOff: float = 0.0
    MV_Chg_Spot: float = 0.0
    MV_Chg_RFR: float = 0.0
    MV_Chg_Dvd: float = 0.0
    MV_Chg_Vol: float = 0.0
    MV_Chg_Time: float = 0.0
    MV_EoP: float = 0.0

    
    def update_mkt_attrib(self, mkt_attrib: MktChgAttrib):
        for f in fields(mkt_attrib):
            object.__setattr__(self, f.name, getattr(mkt_attrib, f.name))

    @classmethod
    def field_names(cls):
        return [f.name for f in fields(cls)]
    
    @property
    def ttl_chgs(self):
        return sum(getattr(self, f.name) for f in fields(self) if str(f.name).startswith('MV_Chg'))
    
    @property
    def EoP_less_BoP(self):
        return self.MV_EoP - self.MV_BoP
    
    # @property
    def check(self):
        if round(self.EoP_less_BoP, 2)  == round(self.ttl_chgs, 2):
            return True
        else:
            print(f'Check Failed! The total of chgs of {self.ttl_chgs:,.0f} does not match the diff of {self.EoP_less_BoP:,.0f} between BoP of {self.MV_BoP:,.0f} and EoP of {self.MV_EoP:,.0f}')
            return False
        
    @property
    def mv_bop_after_ntnl_chgs(self):
        return self.MV_BoP + self.MV_Chg_Ntnl_Added + self.MV_Chg_Ntnl_Chg + self.MV_Chg_Ntnl_Decr + self.MV_Chg_Ntnl_Matured + self.MV_Chg_PayOff
        
    # @property
    def to_dict(self):
        if not self.check:
            print('Warning!  Check failed!')

        # Get all the fields
        results_dict = {f.name: getattr(self, f.name) for f in fields(self)}

        # Add Chgs and Checks
        # ttl_chg_items, mv_chg = self.ttl_chgs, self.EoP_less_BoP
        # results_dict['Ttl_Chgs'] = ttl_chg_items
        # results_dict['MV_EoP-MV_BoP'] = mv_chg
        results_dict['Check'] = self.EoP_less_BoP - self.ttl_chgs

        return results_dict



