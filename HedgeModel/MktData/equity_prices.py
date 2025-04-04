from datetime import date
from pathlib import Path
import pandas as pd
import math

pd.options.display.float_format = "{:,.2f}".format

class EquityPrices:
    
    __price_data_path = Path(Path(__file__).parents[2], "MktData")
    __price_data_file = 'OrionWinterfell_Index_Prices.csv'

    def __init__(self):

        self.px_dict = self.read_price_file()

    def read_price_file(self):

        price_file = Path(self.__price_data_path, self.__price_data_file)
        px_df = pd.read_csv(price_file)
        px_df['AsOfDate'] = pd.to_datetime(px_df['AsOfDate']).dt.date

        px_dict = px_df.set_index('AsOfDate').to_dict('index')

        return px_dict

    
    def get_px(self, asofdt: date):

        px_by_idx = self.px_dict.get(asofdt, None)

        if px_by_idx is None:
            print(f'No prices found on {asofdt}')

        return px_by_idx
    
    def get_px(self, asofdt: date, idx: str = None):
        
        px_by_idx = self.px_dict.get(asofdt, None)

        if px_by_idx is None:
            print(f'No prices found on {asofdt}')
            return
        else:
        
            if idx is None:
                return px_by_idx
            else:

                px = px_by_idx.get(idx, None)
                
                if px is None or math.isnan(px):
                    print(f'No prices for {idx} on {asofdt}')
                    return

                return px        
