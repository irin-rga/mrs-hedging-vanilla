from enum import StrEnum

class OptType(StrEnum):
    
    Put = "Put"
    Call = "Call"
    Put_Spread = "Put_Spread"
    Call_Spread = "Call Spread"        
    Digital = "Digital"
    Digital_Put = "Digital Put"
    Digital_Call = "Digital Call"
    StepUp = "SU"
    DualDirection = "DD"