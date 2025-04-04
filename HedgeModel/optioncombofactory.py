from HedgeModel.vanilla.digitalcall import DigitalCall
from HedgeModel.vanilla.digitalput import DigitalPut
from HedgeModel.vanilla.vanilla import Vanilla
from HedgeModel.vanilla.call import Call
from HedgeModel.vanilla.put import Put
from HedgeModel.opttype import OptType
from datetime import date
from typing import List


def create_put(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float):
    
    put = Put(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt)
    return [put]
    
def create_call(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float) -> List['Vanilla']:
        
    call = Call(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt)

    opt_list = [call]

    if buffer != 1.0:
        name = (f"{buffer*100:.0f}%_Buffer")
        put = Put(name=name, position_mult=-1.0, idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=(1-buffer)*k_low, exp_dt=ExpDt)
        opt_list.append(put)

    return opt_list

    
def create_put_spread(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float):    
    raise NotImplementedError('No Options of this type currently exist for Orion or Winterfell!')

def create_call_spread(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float) -> List['Vanilla']:
        
    long_call = Call(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt)
    opt_list = [long_call]

    if rate != 9.9999:
        short_call = Call(idx=Bbg_Idx, idx_lvl_start=IdxLvl_StartDt, position_mult=-1.0, strike=k_high, exp_dt = ExpDt)
        opt_list.append(short_call)

    if buffer != 1.0:
        name = (f"{buffer*100:.0f}%_Buffer")
        put = Put(name=name, position_mult=-1.0, idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=(1-buffer)*k_low, exp_dt=ExpDt)
        opt_list.append(put)
    
    return opt_list


def create_digital(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float) -> List['Vanilla']:
        
    long_digital = DigitalCall(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt, rate=rate)

    opt_list = [long_digital]

    if buffer != 1.0:
        name = (f"{buffer*100:.0f}%_Buffer")
        put = Put(name=name, position_mult=-1.0, idx=Bbg_Idx, strike=(1-buffer)*k_low, exp_dt=ExpDt)
        opt_list.append(put)

    return opt_list

def create_digital_put(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float) -> List['Vanilla']:
        
    digital_put = DigitalPut(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt, rate=rate)

    return [digital_put]

def create_digital_call(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float) -> List['Vanilla']:
    
    long_digital = DigitalCall(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt, rate=rate)

    opt_list = [long_digital]

    if buffer != 1.0:
        name = (f"{buffer*100:.0f}%_Buffer")
        put = Put(name=name, position_mult=-1.0, idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=(1-buffer)*k_low, exp_dt=ExpDt)
        opt_list.append(put)

    return opt_list


def create_step_up(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float):
    
    long_digital = DigitalCall(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt, rate=rate)
    opt_list = [long_digital]

    if buffer != 1.0:
        name = (f"{buffer*100:.0f}%_Buffer")
        put = Put(name=name, position_mult=-1.0, idx=Bbg_Idx, idx_lvl_start=IdxLvl_StartDt, strike=(1-buffer)*k_low, exp_dt=ExpDt)
        opt_list.append(put)

    return opt_list



def create_dual_direction(Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float):
    
    long_call = Call(idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low, exp_dt = ExpDt)
    opt_list = [long_call]

    if rate != 9.9999:
        short_call = Call(position_mult=-1.0, idx=Bbg_Idx, strike=k_high, exp_dt = ExpDt)
        opt_list.append(short_call)

    long_atm_put = Put(name='LongATMPut', idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=k_low)
    dbl_short_put = Put(name='DblShortPut_90%', idx_lvl_start=IdxLvl_StartDt, position_mult=-2.0, idx=Bbg_Idx, strike=(1-buffer)*k_low)
    short_digital_put = DigitalPut(name='ShortDigitalPut_90%', position_mult=-1.0, idx_lvl_start=IdxLvl_StartDt, idx=Bbg_Idx, strike=(1-buffer)*k_low, rate=buffer)

    opt_list.extend([long_atm_put, dbl_short_put, short_digital_put])

    return opt_list


creation_function_dict = {
    OptType.Put : create_put,
    OptType.Call : create_call,
    OptType.Put_Spread : create_put_spread,
    OptType.Call_Spread : create_call_spread,
    OptType.Digital : create_digital_call,
    OptType.Digital_Call : create_digital_call,
    OptType.Digital_Put : create_digital_put,
    OptType.DualDirection : create_dual_direction,
    OptType.StepUp : create_step_up,
}


def create_option_combo(OptType : OptType, Bbg_Idx: str, IdxLvl_StartDt: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float):    
    return creation_function_dict[OptType](Bbg_Idx, IdxLvl_StartDt, k_low, k_high, ExpDt, buffer, rate)

# def create_option_combo(OptType : OptType, Bbg_Idx: str, Ntnl: float, contracts: float, k_low: float, k_high: float, ExpDt: date, buffer: float, rate: float):    
#     return creation_function_dict[OptType](Bbg_Idx, Ntnl, contracts, k_low, k_high, ExpDt, buffer, rate)
        

    





