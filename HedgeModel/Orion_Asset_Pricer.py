from HedgeModel.vanilla.blackscholes import BSCall, BSDigitalCall
from datetime import datetime, date
from pathlib import Path
from time import time
from os import path
import pandas as pd
import typing
import os

from MktData.mkt_data import MktData

pd.options.display.float_format = "{:,.2f}".format


class Orion_Asset_Pricer:

    __default_tickers = ['SPMARC5P Index', 'NDX Index', 'SPX Index']
    # __inforce_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\IUL_and_VUL_AV_and_Caps_Monthly_2023.xlsx'
    # __results_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\IUL_and_VUL_AV_and_Caps_Monthly_2023_Results.xlsx'

    # __inforce_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\Winterfell_TestInforce_Mocked_20241209_ValDt.xlsx'
    # __results_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\Winterfell_TestInforce_Mocked_20241209_ValDt_Results.xlsx'

    # __asset_file = r'\\rgare.net\stlfinmkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\Orion_HedgeAsset_Holdings.xlsx'
    __asset_file = r'C:\Users\S0053071\Repos\Orion_Process_Backup\Orion_HedgeAsset_Holdings.xlsx'

    __results_fldr = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Results'

    __results_file = r'C:\Users\S0053071\Repos\Winterfell\IUL_Rpt_File\IUL_Inforce_20241022_Results.xlsx'
    
    
    
    def __init__(self, tickers: typing.Optional[list] = None, asofdt: typing.Optional[date] = None):
        
        self.tickers = self.__default_tickers if tickers is None else tickers
        self.mktdata=MktData(self.tickers)
        self.output_path = None
        self.output = None
        
        # Set AsOfDt to current Date if None
        if asofdt is None:
            asofdt = date.today()

        self.asofdt = asofdt

        # Read Positions
        self.positions = self.read_positions()
        
        # Create Function Map for Pricing Options by Type
        self.opt_type_lu = {
        'Call' : self.price_call_spread,
        'Call Spread' : self.price_call_spread,
        'Digital' : self.price_digital        
        }

        # Choose output greek types
        # self.output_greek_types = ['Price', 'Delta']
        self.output_greek_types = ['Price']

        # Apply Shocks up and down on the given amount
        # self.apply_equity_shocks_on_valdt(0.05)
        
        

        # Run pricing
        # self.run()

    
    def read_positions(self):

        positions = pd.read_excel(self.__asset_file)

        """
        NOTE -- For Orion:
        Segment Start Date: The 1st BD of calendar month
        Segment Expiry Date:  1 year later w/ Prev BD convention
        """

        # TODO:  Need to bifurcate the logic below to handle VUL, which always assumes the 3rd Friday of the month (for both start date and end date)
        position_cols = positions.columns

        if 'HedgeDate' not in position_cols:
            if 'ExpiryDt' not in position_cols or 'Product' not in position_cols:
                print('Neither HedgeDate nor ExpiryDt fields are present in the inforce data!')
                raise Exception('Neither HedgeDate nor ExpiryDt fields are present in the inforce data!')
            else:
                # Calc HedgeDate from ExpiryDt
                positions['ExpiryDt'] = pd.to_datetime(positions['ExpiryDt']).dt.date
                positions['HedgeDate'] = positions.apply(lambda row: self.mktdata.get_Winterfell_StartDt_From_ExpiryDt(row['Product'], row['ExpiryDt'], row['Mat(Yrs)']), axis=1)

        # if 'ExpiryDt' not in position_cols:
        #     if 'HedgeDate' not in position_cols or 'Product' not in position_cols:
        #         print('Either (HedgeDate and Product) or ExpiryDt is missing in the inforce data!')
        #         raise Exception('Either (HedgeDate and Product) or ExpiryDt is missing in the inforce data!')
        #     else:
        #         # Calc ExpiryDt from HedgeDate
        #         positions['HedgeDate'] = pd.to_datetime(positions['HedgeDate']).dt.date
        #         positions['ExpiryDt'] = positions.apply(lambda row: self.mktdata.get_Winterfell_ExpiryDt(row['Product'], row['HedgeDate'], row['Mat(Yrs)']), axis=1)

        if 'AsOfDt' not in position_cols:
            positions['AsOfDt'] = self.asofdt

        # Convert HedgeDate and ValDt to proper dates
        positions['HedgeDate'] = pd.to_datetime(positions['HedgeDate']).dt.date
        positions['ExpiryDt'] = pd.to_datetime(positions['ExpiryDt']).dt.date
        positions['AsOfDt'] = pd.to_datetime(positions['AsOfDt']).dt.date

        # Add Column for Maturity Date of Option and for Idx Price of Option Underlying on HedgeDate <Option Start Dt>
        # inforce['Maturity'] = inforce.apply(lambda row: self.mktdata.get_Winterfell_IUL_Maturity(row['HedgeDate'], row['Mat(Yrs)']), axis=1)

        # Add Price on Start Date if not already in file
        if 'IdxLvl_StartDt' not in position_cols:
            positions['IdxLvl_StartDt'] = positions.apply(lambda row: self.mktdata.get_px(row['HedgeDate'], row['Bbg_Idx']), axis=1)    
        # Add Price on Valuation/AsOf Date if not already in file
        if 'IdxLvl_AsOfDt' not in position_cols:
            positions['IdxLvl_AsOfDt'] = positions.apply(lambda row: self.mktdata.get_px(row['AsOfDt'], row['Bbg_Idx']), axis=1)
        # Add Calculated #of Contracts
        if 'Contracts' not in position_cols:
            positions['Contracts'] = positions['Notional'] / positions['IdxLvl_StartDt']
                        
        return positions

    def set_asofdt(self, df: pd.DataFrame, asofdt: typing.Optional[date] = None):
        
        if asofdt:
            self.asofdt = asofdt
        
        df['AsOfDt'] = self.asofdt
        # self.positions['AsOfDt'] = pd.to_datetime(self.positions['AsOfDt']).dt.date
        df['IdxLvl_AsOfDt'] = df.apply(lambda row: self.mktdata.get_px(row['AsOfDt'], row['Bbg_Idx']), axis=1)
    
    def apply_equity_shocks_on_valdt(self, shock_amt=0.05):

        # Add a column called scenario, set equal to base
        self.positions['Scenario'] = 'Base'

        # Clone inforce, update scenario to shock_up_{shock_amt} and set 'IdxLvl_ValDt' = 'IdxLvl_ValDt' * (1+shock_amt)
        shock_up_df = self.positions.copy(deep=True)
        shock_name = 'EqUp_' + str(100*shock_amt) + 'Pct'
        shock_up_df['Scenario'] = shock_name
        shock_up_df['IdxLvl_AsOfDt'] = shock_up_df['IdxLvl_AsOfDt'] * (1+shock_amt)
        
        # Clone inforce, update scenario to shock_dn_{shock_amt} and set 'IdxLvl_ValDt' = 'IdxLvl_ValDt' * (1-shock_amt)
        shock_dn_df = self.positions.copy(deep=True)
        shock_name = 'EqDn_' + str(100*shock_amt) + 'Pct'
        shock_dn_df['Scenario'] = shock_name
        shock_dn_df['IdxLvl_AsOfDt'] = shock_dn_df['IdxLvl_AsOfDt'] * (1-shock_amt)

        # Append shock_up and shock_dn scenarios to inforce
        self.positions = pd.concat([self.positions, shock_up_df, shock_dn_df], ignore_index=True)   

        # return
        return
    
    def init_output_df(self):

        output_df = (self.positions).copy(deep=True)

        # Add columns to self.output in the desired order and initialize to 0.0
        bs_param_output_cols = ['TTM', 'RFR', 'Q', 'IV_K_Lower', 'IV_K_Upper'] 

        # Initialize and Order the BS Params to 0 and place these fields in the output before the pricing results
        for col in bs_param_output_cols:
            output_df[col] = 0.0

        base_output_cols = ['Long_Call', 'Short_Call', 'Long_Digital_Call', 'Total_Opt']

        # Initialize and Order the pricing outputs for each greek and place these new columns at the end of the output dataframe
        for greek_type in self.output_greek_types:
             for col in base_output_cols:
                  output_df[col + self.output_fld_adj(greek_type)] = 0.0

        return output_df

    def run(self, asofdt: typing.Optional[date] = None):

        # TODO: Add logic to create expiry report on an expiration date!

        # Store Results of Process in Separate DF
        self.output = self.init_output_df()

        # Set the AsOfDt field as well as the IdxLvl_AsOfDt
        self.set_asofdt(self.output, asofdt)

        # Create report for option payoff for options on maturity date
        # TODO:  Create function for creation of additional output detailing option payoffs        
        
        # Remove expired options
        self.output = self.output[self.output['ExpiryDt'] >= self.asofdt]

        # TODO: Extend above restriction to also exclude options with HedgeDate > self.asofdt !!!!

        # For Each Val Date  <for this pricing exercise, should only be 1 AsOfDt/ValDt>
        for (asof_dt, idx), df_by_asof_dt_and_idx in self.output.groupby(['AsOfDt', 'Bbg_Idx'], group_keys=False):

            # Make sure it's a date            
            str_asof_dt = asof_dt.strftime("%m/%d/%Y")
            
            # self.logger.info(f'Starting Valuation of {idx} Options on {str_val_dt}')
            print(f'Starting Valuation of {idx} Options on {str_asof_dt}')

            # Load Vol Only 1x per Index & ValDate Combo            
            self.mktdata.load_implied_vol(asof_dt)

            # Log start time (starting after separately tracked vol loading & scenario creation time)
            priced_cnt = 0
            start = time()

            # Just to split by FundID/FundNumber in order to skip if running only a subset of funds (per config)
            for opt_type, group in df_by_asof_dt_and_idx.groupby('Opt_Type', group_keys=False):

                print(f'Starting the pricing of {opt_type} options')
                
                for df_idx, row in group.iterrows():

                    row_opt_type = row['Opt_Type']

                    for greek_type in self.output_greek_types:
                        self.opt_type_lu[row_opt_type](df_idx, row, greek_type)
                                                                                             
                    priced_cnt += 1

                # Log RunTime for this ValDate & Index Combination
                end = time()
                elapsed = end-start
                msg = f"Total Runtime for {priced_cnt} {idx.upper()} options for val_dt {str_asof_dt}: "
                msg = msg + f"{elapsed:0.4f} secs"
                # self.logger.info(msg)
                print(msg)

        # Save Results
        fname = 'Orion_HedgeAssets_' + self.asofdt.strftime("%Y%m%d") + '.xlsx'
        self.save_results(fname, self.output)

        return self.output.copy(deep=True)

    def price_assets_on_trade_dt(self):

        # Store Results of Process in Separate DF
        self.output = self.init_output_df()

        # Update AsOfDt's to TradeDts <after making sure TradeDT is a date object>
        self.output['TradeDt'] = pd.to_datetime(self.output['TradeDt']).dt.date
        self.output['AsOfDt'] = self.output['TradeDt']
        self.output['IdxLvl_AsOfDt'] = self.output.apply(lambda row: self.mktdata.get_px(row['AsOfDt'], row['Bbg_Idx']), axis=1)
        
        # For Each Trade Date, Run the Pricing Exercise on that Trade Date <Can use AsOfDt because AsOfDt set to TradeDt!>
        for (asof_dt, idx), df_by_asof_dt_and_idx in self.output.groupby(['AsOfDt', 'Bbg_Idx'], group_keys=False):

                        
            # self.set_asofdt(df_by_asof_dt)

            # Make sure it's a date            
            str_asof_dt = asof_dt.strftime("%m/%d/%Y")
            
            # self.logger.info(f'Starting Valuation of {idx} Options on {str_val_dt}')
            print(f'Starting Valuation of {idx} Options on {str_asof_dt}')

            # Load Vol Only 1x per Index & ValDate Combo            
            self.mktdata.load_implied_vol(asof_dt)

            # Log start time (starting after separately tracked vol loading & scenario creation time)
            priced_cnt = 0
            start = time()

            # Just to split by FundID/FundNumber in order to skip if running only a subset of funds (per config)
            for opt_type, group in df_by_asof_dt_and_idx.groupby('Opt_Type', group_keys=False):

                print(f'Starting the pricing of {opt_type} options')
                
                for df_idx, row in group.iterrows():

                    row_opt_type = row['Opt_Type']

                    self.opt_type_lu[row_opt_type](df_idx, row, 'Price')
                    self.output.at[df_idx, 'ModelPx_TradeDt'] = self.output.at[df_idx, 'MV']

                    # for greek_type in self.output_greek_types:
                    #     self.opt_type_lu[row_opt_type](df_idx, row, greek_type)
                                                                                             
                    priced_cnt += 1

                # Log RunTime for this ValDate & Index Combination
                end = time()
                elapsed = end-start
                msg = f"Total Runtime for {priced_cnt} {idx.upper()} options for val_dt {str_asof_dt}: "
                msg = msg + f"{elapsed:0.4f} secs"
                # self.logger.info(msg)
                print(msg)

        # Save Results
        fname = 'Orion_HedgeAssets_' + 'Prices_on_TradeDts' + '.xlsx'
        self.save_results(fname, self.output)

        return self.output.copy(deep=True)
    
    def output_fld_adj(self, greek_type):
        return '_' + greek_type
        # if greek_type=='Price':
        #     # return '_Px'
        #     return ''
        # else:
        #     # Assume Delta
        #     return '_Delta'            

    def get_ttm_rfr_q(self, df_idx: int, df: pd.DataFrame):

        # Get key dates and the mkt idx from the dataframe
        asof_dt, exp_dt, idx = df.AsOfDt, df.ExpiryDt, df.Bbg_Idx
        # Get Values for these items from Mkt Objects
        ttm = self.mktdata.get_ttm(asof_dt, exp_dt)
        rfr = self.mktdata.get_rfr(asof_dt, idx, exp_dt)
        q = self.mktdata.get_q(asof_dt, idx, exp_dt)
        # Write Results of these values to output df
        self.output.at[df_idx, 'TTM'] = ttm
        self.output.at[df_idx, 'RFR'] = rfr
        self.output.at[df_idx, 'Q'] = q        
        # return results for their use in Black Scholes Equations
        return ttm, rfr, q
    
    def get_df_vars(self, df_idx: int, df: pd.DataFrame):
        
        # Get Start and ExpiryDt Dates plus Ticker Name
        asof_dt, ticker, exp_dt = df.AsOfDt, df.Bbg_Idx, df.ExpiryDt
        # Get Parameters needed for Adjusted Notional & #of contracts
        contracts = df.Contracts
        # Get Parameters needed for call spread
        
        # Get prices at time 0 and current val date
        s = df.IdxLvl_AsOfDt
        # Set Strikes for Call Spread
        # k_low, k_up = s0, s0 * (1+cap)
        k_low, k_up = df.Strike_Low, df.Strike_High
        # Get IV for ATM and Cap
        iv_low = self.mktdata.get_iv(asof_dt, ticker, exp_dt, k_low)
        iv_up = ''
        
        # Write Results of implied vol to output df
        self.output.at[df_idx, 'IV_K_Lower'] = iv_low
        # For Opt_Types other than Step Up, output the IV at the cap
        if df.Opt_Type == 'Call Spread':
            iv_up = self.mktdata.get_iv(asof_dt, ticker, exp_dt, k_up)
            self.output.at[df_idx, 'IV_K_Upper'] = iv_up

        # return the common variables used in price_call_spread, price_step_up, and price_dual_digital
        return contracts, s, k_low, k_up, iv_low, iv_up
    
    def price_call_spread(self, df_idx: int, df: pd.DataFrame, greek_type='Price'):
        
        # Get t, r & q for Black Scholes Params        
        t, r, q = self.get_ttm_rfr_q(df_idx, df)

        # Initialize other variables from the dataframe
        contracts, s, k_low, k_up, iv_low, iv_up = self.get_df_vars(df_idx, df)
                            
        # Initialize Long & Short Call Prices to Zero
        Long_Call = Short_Call = 0

        # Calc and output price of Long_Call_ATM_Px
        Long_Call = BSCall(s, k_low, r, q, iv_low, t, greek_type) * contracts
        self.output.at[df_idx, 'Long_Call' + self.output_fld_adj(greek_type)] = Long_Call

        if df.Opt_Type == 'Call Spread':
            # Calc and output price of Short_Call_Cap_Px
            Short_Call = BSCall(s, k_up, r, q, iv_up, t, greek_type) * contracts
            self.output.at[df_idx, 'Short_Call' + self.output_fld_adj(greek_type)] = Short_Call
                                        
        # Calculate Final Payoff in $$$
        Total = Long_Call - Short_Call
        
        if greek_type=='Price' and 'MV' in self.output.columns:
            self.output.at[df_idx, 'MV'] = Total
        else:
            self.output.at[df_idx, 'Total_Opt' + self.output_fld_adj(greek_type)] = Total

    def price_digital(self, df_idx: int, df: pd.DataFrame, greek_type='Price'):
        
        # ['TTM', 'RFR', 'Q', 'IV_ATM', 'IV_Cap', 'IV_Buffer', 'Long_Call_ATM_Px', 'Short_Call_Cap_Px', 'Long_Digital_Call_ATM_Px', 'Short_Digital_Put_Buffer_Px', 'Short_Put_Buffer_Px', 'Long_Put_ATM_Px', 'Dbl_Short_Put_Buffer_Px', 'Final_Opt_Px_Pct', 'Final_Opt_Px']
        t, r, q = self.get_ttm_rfr_q(df_idx, df)
        
        # Initialize other variables from the dataframe
        contracts, s, k_low, k_up, iv_low, iv_up = self.get_df_vars(df_idx, df)
                
        # Get the rate for the digital from the cap <cap is the upside digital payoff rate for Step Up>
        ntnl, rate = df['Notional'], df['Cap/Rate']        
        payoff = ntnl * rate

        Long_Digital_Call = BSDigitalCall(s, k_low, r, q, iv_low, t, payoff, greek_type)
        self.output.at[df_idx, 'Long_Digital_Call' + self.output_fld_adj(greek_type)] = Long_Digital_Call

        if greek_type=='Price' and 'MV' in self.output.columns:
            self.output.at[df_idx, 'MV'] = Long_Digital_Call
        else:
            self.output.at[df_idx, 'Total_Opt' + self.output_fld_adj(greek_type)] = Long_Digital_Call

    def create_output_fldr(self):
        
        # output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m%d'))
        output_path = path.join(self.__results_fldr, self.asofdt.strftime('%Y%m'))

        if not path.exists(output_path):
            os.makedirs(output_path)

        return output_path

    def save_results(self, df_filename: str, df: pd.DataFrame, keep_idx: bool=False, idx_lbl: str=None):

        if self.output_path is None:
            self.output_path = self.create_output_fldr()
                
        print(f'Saving {df_filename} to: {self.output_path}')        
        
        full_path = path.join(self.output_path, df_filename)            
        
        save_start = time()

        if df_filename.endswith('.csv'):
            df.to_csv(full_path, index=keep_idx, index_label=idx_lbl)
        elif df_filename.endswith('.xlsx'):
            df.to_excel(full_path, index=keep_idx, index_label=idx_lbl)

        save_end = time()
        save_time = save_end - save_start
        print(f"Time spend saving {df_filename}: {int(save_time // 60)} mins {int(save_time % 60)} secs")


if __name__ == "__main__":

    time_ms = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f'Starting Orion Asset Pricing at: {time_ms}')
    start = time()

    pricer = Orion_Asset_Pricer()

    # pricer.price_assets_on_trade_dt()

    asofdt = date(2024, 12, 31)
    pricer.run(asofdt)


    end = time()
    elapsed = end-start
    msg = f"Total Runtime for Winterfell Option Pricing: "
    msg = msg + f"{elapsed:0.4f} secs"
    # self.logger.info(msg)
    print(msg)

    time_ms = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f'Finished Winterfell Pricing at: {time_ms}')
    
    print()

