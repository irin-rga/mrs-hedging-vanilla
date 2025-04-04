from ..vanilla.blackscholes import BSCall, BSPut, BSDigitalCall, BSDigitalPut
from datetime import datetime, date
from pathlib import Path
from time import time
import pandas as pd
import typing
import os

from ..MktData.mkt_data import MktData

pd.options.display.float_format = "{:,.2f}".format


class Winterfell_Pricing:

    __default_tickers = ['MXEA Index', 'RTY Index', 'SPX Index']
    # __inforce_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\IUL_and_VUL_AV_and_Caps_Monthly_2023.xlsx'
    # __results_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\IUL_and_VUL_AV_and_Caps_Monthly_2023_Results.xlsx'

    # __inforce_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\Winterfell_TestInforce_Mocked_20241209_ValDt.xlsx'
    # __results_file = r'C:\Users\S0053071\Repos\Winterfell\2023_BudgetRecon\Winterfell_TestInforce_Mocked_20241209_ValDt_Results.xlsx'

    # Old Format
    # __inforce_file = r'C:\Users\S0053071\Repos\Winterfell\IUL_Rpt_File\IUL_Inforce_20241022.xlsx'
    # __results_file = r'C:\Users\S0053071\Repos\Winterfell\IUL_Rpt_File\IUL_Inforce_20241022_Results.xlsx'

    # EQH's Liability Segment File Format
    # NOTE: the inforce file below is the raw file r'C:\Users\S0053071\Repos\Winterfell\Positions\20241231\TG_PCS_Combined_20241231_RGA.csv'
    #       processed using prototypes\winterfell\LiabilitySegmentReader.ipynb
    __inforce_file = r'C:\Users\S0053071\Repos\Winterfell\Positions\20241231\Winterfell_LiabilitySegments_20241231.csv'
    __results_file = r'C:\Users\S0053071\Repos\Winterfell\Positions\20241231\Winterfell_PricedLiabilitySegments_20241231.xlsx'
    
        
    def __init__(self, tickers: typing.Optional[list] = None):
        
        self.tickers = self.__default_tickers if tickers is None else tickers
        self.mktdata=MktData(self.tickers)

        # Read Inforce
        # self.inforce = self.read_inforce_data_old_format()
        self.inforce = self.read_inforce()
        
        # Create Function Map for Pricing Options by Type
        self.opt_type_lu = {
        'Call Spread' : self.price_call_spread,
        'DD' : self.price_dual_direction,
        'SU' : self.price_step_up
        }

        # Choose output greek types
        self.output_greek_types = ['Price', 'Delta']
        # self.output_greek_types = ['Price']

        # Apply Shocks up and down on the given amount
        # self.apply_equity_shocks_on_valdt(0.05)
        
        # Store Results of Process in Separate DF        
        self.output = self.init_output_df()

        # Run pricing
        self.run()


    def read_inforce(self):

        inforce = pd.read_csv(self.__inforce_file)

        # TODO:  Need to bifurface the logic below to handle VUL, which always assumes the 3rd Friday of the month (for both start date and end date)
        inforce_cols = inforce.columns


        inforce['Seg_EndDt'] = pd.to_datetime(inforce['Seg_EndDt']).dt.date
        inforce['ExpiryDt'] = pd.to_datetime(inforce['Seg_EndDt']).dt.date

        if 'Seg_StartDt' not in inforce_cols:
            inforce['Seg_StartDt'] = inforce.apply(lambda row: self.mktdata.get_Winterfell_StartDt_From_ExpiryDt(row['Product'], row['ExpiryDt'], row['Mat(Yrs)']), axis=1)

        # Add ValDate to dataframe <Note: This will not be preferred approach when pricing the List['position']
        val_dt = date(2024, 12, 31)
        inforce['ValDt'] = val_dt

        # Just to make double sure the values are dates
        inforce['Seg_StartDt'] = pd.to_datetime(inforce['Seg_StartDt']).dt.date
        inforce['ValDt'] = pd.to_datetime(inforce['ValDt']).dt.date

        # Add Index Price on the ValDt as well as 'Adj_Ntnl <for consistency w/ prior pricing
        inforce['IdxLvl_ValDt'] = inforce.apply(lambda row: self.mktdata.get_px(row['ValDt'], row['Bbg_Idx']), axis=1)
        inforce['Adj_Ntnl'] = inforce['Notional']

        return inforce


    def read_inforce_data_old_format(self):

        inforce = pd.read_excel(self.__inforce_file)

        """
        NOTE -- For Winterfell:
        Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
        Segment Expiry Date:  The 1st BD occuring after the 13th calendar day of the month
        """

        # TODO:  Need to bifurface the logic below to handle VUL, which always assumes the 3rd Friday of the month (for both start date and end date)
        inforce_cols = inforce.columns

        if 'HedgeDate' not in inforce_cols:
            if 'ExpiryDt' not in inforce_cols or 'Product' not in inforce_cols:
                print('Neither HedgeDate nor ExpiryDt fields are present in the inforce data!')
                raise Exception('Neither HedgeDate nor ExpiryDt fields are present in the inforce data!')
            else:
                # Calc HedgeDate from ExpiryDt
                inforce['ExpiryDt'] = pd.to_datetime(inforce['ExpiryDt']).dt.date
                inforce['HedgeDate'] = inforce.apply(lambda row: self.mktdata.get_Winterfell_StartDt_From_ExpiryDt(row['Product'], row['ExpiryDt'], row['Mat(Yrs)']), axis=1)

        if 'ExpiryDt' not in inforce_cols:
            if 'HedgeDate' not in inforce_cols or 'Product' not in inforce_cols:
                print('Either (HedgeDate and Product) or ExpiryDt is missing in the inforce data!')
                raise Exception('Either (HedgeDate and Product) or ExpiryDt is missing in the inforce data!')
            else:
                # Calc ExpiryDt from HedgeDate
                inforce['HedgeDate'] = pd.to_datetime(inforce['HedgeDate']).dt.date
                inforce['ExpiryDt'] = inforce.apply(lambda row: self.mktdata.get_Winterfell_ExpiryDt(row['Product'], row['HedgeDate'], row['Mat(Yrs)']), axis=1)


        # Convert HedgeDate and ValDt to proper dates
        inforce['HedgeDate'] = pd.to_datetime(inforce['HedgeDate']).dt.date
        inforce['ExpiryDt'] = pd.to_datetime(inforce['ExpiryDt']).dt.date
        inforce['ValDt'] = pd.to_datetime(inforce['ValDt']).dt.date

        # Add Column for Maturity Date of Option and for Idx Price of Option Underlying on HedgeDate <Option Start Dt>
        # inforce['Maturity'] = inforce.apply(lambda row: self.mktdata.get_Winterfell_IUL_Maturity(row['HedgeDate'], row['Mat(Yrs)']), axis=1)

        inforce['IdxLvl_StartDt'] = inforce.apply(lambda row: self.mktdata.get_px(row['HedgeDate'], row['Bbg_Idx']), axis=1)
        inforce['IdxLvl_ValDt'] = inforce.apply(lambda row: self.mktdata.get_px(row['ValDt'], row['Bbg_Idx']), axis=1)
        inforce['Adj_Ntnl'] = inforce['Notional']*inforce['Part']
        inforce['Contracts'] = inforce['Adj_Ntnl'] / inforce['IdxLvl_StartDt']

        return inforce

    
    def apply_equity_shocks_on_valdt(self, shock_amt=0.05):

        # Add a column called scenario, set equal to base
        self.inforce['Scenario'] = 'Base'

        # Clone inforce, update scenario to shock_up_{shock_amt} and set 'IdxLvl_ValDt' = 'IdxLvl_ValDt' * (1+shock_amt)
        shock_up_df = self.inforce.copy(deep=True)
        shock_name = 'EqUp_' + str(100*shock_amt) + 'Pct'
        shock_up_df['Scenario'] = shock_name
        shock_up_df['IdxLvl_ValDt'] = shock_up_df['IdxLvl_ValDt'] * (1+shock_amt)
        
        # Clone inforce, update scenario to shock_dn_{shock_amt} and set 'IdxLvl_ValDt' = 'IdxLvl_ValDt' * (1-shock_amt)
        shock_dn_df = self.inforce.copy(deep=True)
        shock_name = 'EqDn_' + str(100*shock_amt) + 'Pct'
        shock_dn_df['Scenario'] = shock_name
        shock_dn_df['IdxLvl_ValDt'] = shock_dn_df['IdxLvl_ValDt'] * (1-shock_amt)

        # Append shock_up and shock_dn scenarios to inforce
        self.inforce = pd.concat([self.inforce, shock_up_df, shock_dn_df], ignore_index=True)   

        # return
        return
    
    def init_output_df(self):

        output_df = (self.inforce).copy()

        # Add columns to self.output in the desired order and initialize to 0.0
        bs_param_output_cols = ['TTM', 'RFR', 'Q', 'IV_ATM', 'IV_Cap', 'IV_Buffer'] 

        # Initialize and Order the BS Params to 0 and place these fields in the output before the pricing results
        for col in bs_param_output_cols:
            output_df[col] = 0.0

        base_output_cols = ['Long_Call_ATM', 'Short_Call_Cap', 'Long_Digital_Call_ATM', 'Short_Digital_Put_Buffer', 'Short_Put_Buffer', 'Long_Put_ATM', 'Dbl_Short_Put_Buffer', 'Final_Opt_Pct', 'Final_Opt']

        # Initialize and Order the pricing outputs for each greek and place these new columns at the end of the output dataframe
        for greek_type in self.output_greek_types:
             for col in base_output_cols:
                  output_df[col + self.output_fld_adj(greek_type)] = 0.0

        return output_df

    def run(self):

        # For Each Val Date
        for (val_dt, idx), df_by_val_dt_and_idx in self.output.groupby(['ValDt', 'Bbg_Idx'], group_keys=False):

            # Make sure it's a date            
            str_val_dt = val_dt.strftime("%m/%d/%Y")
            
            # self.logger.info(f'Starting Valuation of {idx} Options on {str_val_dt}')
            print(f'Starting Valuation of {idx} Options on {str_val_dt}')

            # Load Vol Only 1x per Index & ValDate Combo            
            self.mktdata.load_implied_vol(val_dt)

            # Log start time (starting after separately tracked vol loading & scenario creation time)
            priced_cnt = 0
            start = time()

            # Just to split by FundID/FundNumber in order to skip if running only a subset of funds (per config)
            for opt_type, group in df_by_val_dt_and_idx.groupby('Opt_Type', group_keys=False):

                print(f'Starting the pricing of {opt_type} options')
                
                for df_idx, row in group.iterrows():

                    row_opt_type = row['Opt_Type']

                    for greek_type in self.output_greek_types:
                        self.opt_type_lu[row_opt_type](df_idx, row, greek_type)
                
                    # if opt_type == 'Call Spread':                                                                

                    priced_cnt += 1

                # Log RunTime for this ValDate & Index Combination
                end = time()
                elapsed = end-start
                msg = f"Total Runtime for {priced_cnt} {idx.upper()} options for val_dt {str_val_dt}: "
                msg = msg + f"{elapsed:0.4f} secs"
                # self.logger.info(msg)
                print(msg)

        # Save Results
        self.save_results()

    def output_fld_adj(self, greek_type):
        if greek_type=='Price':
            return '_Px'
        else:
            # Assume Delta
            return '_Delta'            


    def get_ttm_rfr_q(self, df_idx: int, df: pd.DataFrame):

        # Get Values for these items from Mkt Objects
        ttm = self.mktdata.get_ttm(df['ValDt'], df['ExpiryDt'])
        rfr = self.mktdata.get_rfr(df['ValDt'], df['Bbg_Idx'], df['ExpiryDt'])
        q = self.mktdata.get_q(df.ValDt, df.Bbg_Idx, df.ExpiryDt)

        # Write Results of these values to output df
        self.output.at[df_idx, 'TTM'] = ttm
        self.output.at[df_idx, 'RFR'] = rfr
        self.output.at[df_idx, 'Q'] = q
        
        # return results for their use in Black Scholes Equations
        return ttm, rfr, q
    
    def get_df_vars(self, df_idx: int, df: pd.DataFrame):
        
        # Get Start and ExpiryDt Dates plus Ticker Name
        val_dt, ticker, mat_dt = df.ValDt, df.Bbg_Idx, df.ExpiryDt
        # Get Parameters needed for Adjusted Notional & #of contracts
        adj_ntnl, contracts = df.Adj_Ntnl, df.Contracts
        # Get Parameters needed for call spread and <potentially> buffer
        cap, buffer = df.Cap, df.Buffer
        # Get prices at time 0 and current val date
        s, s0 = df.IdxLvl_ValDt, df.IdxLvl_StartDt
        # Set Strikes for Call Spread
        k_atm, k_cap = s0, s0 * (1+cap)
        # Get IV for ATM and Cap
        iv_atm = self.mktdata.get_iv(val_dt, ticker, mat_dt, k_atm)
        iv_cap = self.mktdata.get_iv(val_dt, ticker, mat_dt, k_cap)
        # Write Results of implied vol to output df
        self.output.at[df_idx, 'IV_ATM'] = iv_atm
        # For Opt_Types other than Step Up, output the IV at the cap
        if df.Opt_Type != 'SU':
            self.output.at[df_idx, 'IV_Cap'] = iv_cap

        # return the common variables used in price_call_spread, price_step_up, and price_dual_digital
        return val_dt, ticker, mat_dt, adj_ntnl, contracts, cap, buffer, s, k_atm, k_cap, iv_atm, iv_cap
    
    def price_call_spread(self, df_idx: int, df: pd.DataFrame, greek_type='Price'):
        
        # Get t, r & q for Black Scholes Params        
        t, r, q = self.get_ttm_rfr_q(df_idx, df)

        # Initialize other variables from the dataframe
        val_dt, ticker, mat_dt, adj_ntnl, contracts, cap, buffer, s, k_atm, k_cap, iv_atm, iv_cap = self.get_df_vars(df_idx, df)
                            
        # Set price of buffer to zero and update it if product actually has a buffer.  Initialize Long & Short Call Prices to Zero as well
        Long_Call_ATM_Px = Short_Call_Cap_Px = Short_Put_Buffer_Px = 0

        # Calc and output price of Long_Call_ATM_Px
        Long_Call_ATM_Px = BSCall(s, k_atm, r, q, iv_atm, t, greek_type) * contracts
        self.output.at[df_idx, 'Long_Call_ATM' + self.output_fld_adj(greek_type)] = Long_Call_ATM_Px

        # Calc and output price of Short_Call_Cap_Px
        Short_Call_Cap_Px = BSCall(s, k_cap, r, q, iv_cap, t, greek_type) * contracts
        self.output.at[df_idx, 'Short_Call_Cap' + self.output_fld_adj(greek_type)] = Short_Call_Cap_Px

        # Calc Short_Put_Buffer_Px if needed
        if buffer != 1:

            # Get IV for Buffer
            k_buff = k_atm * (1-buffer)
            iv_buff = self.mktdata.get_iv(val_dt, ticker, mat_dt, k_buff)
            # Write Results of implied vol for bugger to  output df
            self.output.at[df_idx, 'IV_Buffer'] = iv_buff

            # Calc and output price of Short_Call_Cap_Px
            Short_Put_Buffer_Px = BSPut(s, k_buff, r, q, iv_buff, t, greek_type) * contracts
            self.output.at[df_idx, 'Short_Put_Buffer' + self.output_fld_adj(greek_type)] = Short_Put_Buffer_Px

        # Calculate Final Payoff from sum of parts
        Final_Opt_Px_Pct = (Long_Call_ATM_Px - Short_Call_Cap_Px - Short_Put_Buffer_Px) / adj_ntnl
        self.output.at[df_idx, 'Final_Opt_Pct' + self.output_fld_adj(greek_type)] = Final_Opt_Px_Pct
                        
        # Calculate Final Payoff in $$$
        Final_Opt_Px = Final_Opt_Px_Pct * adj_ntnl
        self.output.at[df_idx, 'Final_Opt' + self.output_fld_adj(greek_type)] = Final_Opt_Px                        

    def price_step_up(self, df_idx: int, df: pd.DataFrame, greek_type='Price'):
        
        # ['TTM', 'RFR', 'Q', 'IV_ATM', 'IV_Cap', 'IV_Buffer', 'Long_Call_ATM_Px', 'Short_Call_Cap_Px', 'Long_Digital_Call_ATM_Px', 'Short_Digital_Put_Buffer_Px', 'Short_Put_Buffer_Px', 'Long_Put_ATM_Px', 'Dbl_Short_Put_Buffer_Px', 'Final_Opt_Px_Pct', 'Final_Opt_Px']
        t, r, q = self.get_ttm_rfr_q(df_idx, df)
        
        # Initialize other variables from the dataframe
        val_dt, ticker, mat_dt, adj_ntnl, contracts, cap, buffer, s, k_atm, k_cap, iv_atm, iv_cap = self.get_df_vars(df_idx, df)
                
        # Set price of buffer to zero and update it if product actually has a buffer.  Initialize Long & Short Call Prices to Zero as well
        Long_Digital_Call_ATM_Px = Short_Put_Buffer_Px = 0

        # Get the rate for the digital from the cap <cap is the upside digital payoff rate for Step Up>
        rate = cap

        Long_Digital_Call_ATM_Px = BSDigitalCall(s, k_atm, r, q, iv_atm, t, rate, greek_type) * df.Adj_Ntnl
        self.output.at[df_idx, 'Long_Digital_Call_ATM' + self.output_fld_adj(greek_type)] = Long_Digital_Call_ATM_Px

        # Calc Short_Put_Buffer_Px
        # Get IV for Buffer
        k_buff = k_atm * (1-buffer)
        iv_buff = self.mktdata.get_iv(val_dt, ticker, mat_dt, k_buff)
        # Write Results of implied vol for bugger to  output df
        self.output.at[df_idx, 'IV_Buffer'] = iv_buff

        # Calc and output price of Short_Put_Buffer_Px
        Short_Put_Buffer_Px = BSPut(s, k_buff, r, q, iv_buff, t, greek_type) * contracts
        self.output.at[df_idx, 'Short_Put_Buffer' + self.output_fld_adj(greek_type)] = Short_Put_Buffer_Px

        # Calculate Final Payoff from sum of parts
        Final_Opt_Px_Pct = (Long_Digital_Call_ATM_Px - Short_Put_Buffer_Px) / adj_ntnl
        self.output.at[df_idx, 'Final_Opt_Pct' + self.output_fld_adj(greek_type)] = Final_Opt_Px_Pct

        # Get ntnl adjusted for participation, and calculate units (# of option contracts) in order to calculate payoff in $'s                
        # units = ntnl / s0

        # Calculate Final Payoff in $$$
        Final_Opt_Px = Final_Opt_Px_Pct * adj_ntnl
        self.output.at[df_idx, 'Final_Opt' + self.output_fld_adj(greek_type)] = Final_Opt_Px

    def price_dual_direction(self, df_idx: int, df: pd.DataFrame, greek_type='Price'):
        
        # ['TTM', 'RFR', 'Q', 'IV_ATM', 'IV_Cap', 'IV_Buffer', 'Long_Call_ATM_Px', 'Short_Call_Cap_Px', 'Long_Digital_Call_ATM_Px', 'Short_Digital_Put_Buffer_Px', 'Short_Put_Buffer_Px', 'Long_Put_ATM_Px', 'Dbl_Short_Put_Buffer_Px', 'Final_Opt_Px_Pct', 'Final_Opt_Px']
        t, r, q = self.get_ttm_rfr_q(df_idx, df)
        
        # Initialize other variables from the dataframe
        val_dt, ticker, mat_dt, adj_ntnl, contracts, cap, buffer, s, k_atm, k_cap, iv_atm, iv_cap = self.get_df_vars(df_idx, df)
        
        # Set price of buffer to zero and update it if product actually has a buffer.  Initialize Long & Short Call Prices to Zero as well
        Long_Call_ATM_Px = Short_Call_Cap_Px = Dbl_Short_Put_Buffer_Px = Long_Put_ATM_Px = Short_Digital_Put_Buffer_Px = Short_Put_Buffer_Px = 0

        # Calc and output price of Long_Call_ATM_Px
        Long_Call_ATM_Px = BSCall(s, k_atm, r, q, iv_atm, t, greek_type) * contracts
        self.output.at[df_idx, 'Long_Call_ATM' + self.output_fld_adj(greek_type)] = Long_Call_ATM_Px

        # Calc and output price of Short_Call_Cap_Px
        Short_Call_Cap_Px = BSCall(s, k_cap, r, q, iv_cap, t, greek_type) * contracts
        self.output.at[df_idx, 'Short_Call_Cap' + self.output_fld_adj(greek_type)] = Short_Call_Cap_Px

        # Calc and output price of Long_Put_ATM_Px
        Long_Put_ATM_Px = BSPut(s, k_atm, r, q, iv_atm, t, greek_type) * contracts
        self.output.at[df_idx, 'Long_Put_ATM' + self.output_fld_adj(greek_type)] = Long_Put_ATM_Px

        # Calc Short_Put_Buffer_Px if needed        
        # Get IV for Buffer
        k_buff = k_atm * (1-buffer)
        iv_buff = self.mktdata.get_iv(val_dt, ticker, mat_dt, k_buff)
        # Write Results of implied vol for bugger to  output df
        self.output.at[df_idx, 'IV_Buffer'] = iv_buff

        # Calc and output price of Short_Put_Buffer_Px
        Short_Put_Buffer_Px = BSPut(s, k_buff, r, q, iv_buff, t, greek_type) * contracts
        self.output.at[df_idx, 'Short_Put_Buffer' + self.output_fld_adj(greek_type)] = Short_Put_Buffer_Px
        # Calc and output price of Dbl_Short_Put_Buffer_Px
        Dbl_Short_Put_Buffer_Px = 2 * Short_Put_Buffer_Px
        self.output.at[df_idx, 'Dbl_Short_Put_Buffer' + self.output_fld_adj(greek_type)] = Dbl_Short_Put_Buffer_Px

        # The rate for the digital put is the same as the buffer
        rate = buffer
        
        Short_Digital_Put_Buffer_Px = BSDigitalPut(s, k_buff, r, q, iv_buff, t, rate, greek_type) * adj_ntnl
        self.output.at[df_idx, 'Short_Digital_Put_Buffer' + self.output_fld_adj(greek_type)] = Short_Digital_Put_Buffer_Px

        # Calculate Final Payoff from sum of parts
        Final_Opt_Px_Pct = ((Long_Call_ATM_Px - Short_Call_Cap_Px) + (Long_Put_ATM_Px - Short_Digital_Put_Buffer_Px - Dbl_Short_Put_Buffer_Px)) / adj_ntnl
        self.output.at[df_idx, 'Final_Opt_Pct' + self.output_fld_adj(greek_type)] = Final_Opt_Px_Pct

        # Get ntnl adjusted for participation, and calculate units (# of option contracts) in order to calculate payoff in $'s

        # units = ntnl / s0
        Final_Opt_Px = Final_Opt_Px_Pct * adj_ntnl
        self.output.at[df_idx, 'Final_Opt' + self.output_fld_adj(greek_type)] = Final_Opt_Px 


    def save_results(self):

        # Add a column of 'Final_Opt_Delta_Pct' to enable Delta impact of an X% change in spot by multiplying by X%
        if 'Delta' in self.output_greek_types:
            self.output['Final_Opt_Delta_Pct'] = self.output['Final_Opt_Delta'] * self.output['IdxLvl_ValDt']

        self.output.to_excel(self.__results_file, index=False)


if __name__ == "__main__":

    time_ms = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f'Starting Winterfell Pricing at: {time_ms}')
    start = time()


    pricing = Winterfell_Pricing()

    end = time()
    elapsed = end-start
    msg = f"Total Runtime for Winterfell Option Pricing: "
    msg = msg + f"{elapsed:0.4f} secs"
    # self.logger.info(msg)
    print(msg)

    time_ms = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f'Finished Winterfell Pricing at: {time_ms}')
    
    print()


# NOTE:  TO RUN THIS FILE FROM COMMAND LINE, USE THE FOLLOWING (assuming folder open at the 1_Code folder level):
# python -m HedgeModel.Winterfell.Winterfell_Pricing