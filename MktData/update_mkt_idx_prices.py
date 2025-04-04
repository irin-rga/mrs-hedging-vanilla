#! python3
import datetime as dt
import pandas as pd
from xbbg import blp
import blpapi
import os

  
def update_mkt_idx_prices():

    # MktData_Dir = './MktData/'
    # MktData_Dir = './'
    MktData_Dir = os.path.dirname(os.path.abspath(__file__))
    file_name = 'OrionWinterfell_Index_Prices.csv'

    mkt_file = os.path.join(MktData_Dir, file_name)

    # First get existing prices from file
    price_df = pd.read_csv(mkt_file)

    # Make sure the field is a date
    price_df['AsOfDate'] = pd.to_datetime(price_df['AsOfDate']).dt.date

    # Get dates for which price data is not already present
    last_date = price_df['AsOfDate'].max()

    # Subtract 1day because we want latest end of day prices, and they may not be ready for today
    end_dt = dt.datetime.today() + pd.DateOffset(days=-1)

    # Convert 1st and last dates to strings
    last_date = last_date.strftime("%Y-%m-%d")
    end_dt = end_dt.strftime("%Y-%m-%d")
    # Get other needed bdh params
    tickers = price_df.columns[1:]
    fields = ['Last_Price']

    # Get Missing Data
    new_price_df = blp.bdh(tickers=tickers, flds=fields, start_date=last_date, end_date=end_dt)
    new_price_df.index.name = 'AsOfDate'
    new_price_df.columns = new_price_df.columns.get_level_values(0)
    new_price_df = new_price_df.reset_index()
    new_price_df['AsOfDate'] = pd.to_datetime(new_price_df['AsOfDate']).dt.date

    # Concat the dataframes together
    combinded_px_df = pd.concat([price_df, new_price_df], ignore_index=True)

    # Drop duplicates in case this is run more than once in a day!
    combinded_px_df = combinded_px_df.drop_duplicates()

    # Save to csv
    combinded_px_df.to_csv(mkt_file, index=False)

    print('Mkt Prices Successfully loaded and saved!')
    
if __name__ == "__main__":

    update_mkt_idx_prices()
    