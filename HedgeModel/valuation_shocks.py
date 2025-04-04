from dataclasses import dataclass, fields
from datetime import date
from decimal import DivisionByZero

@dataclass(frozen=True)
class ValuationShocks:
    
    Run: int
    # Val_Dt : date
    ShockName: str
    ShockType: str
    ShockAmt: float
    

    @classmethod
    def field_names(cls):
        return [f.name for f in fields(cls)]
    
    @property
    def price_field(self):
        'Price_' + self.shock_descript

    @property
    def impact_field(self):
        'Impact_' + self.shock_descript

    def shock_descript(self):
        if self.ShockName.upper() == 'BASE':
            return self.ShockName
        else:
            return f"{self.ShockType}Shock_{self.ShockName}"