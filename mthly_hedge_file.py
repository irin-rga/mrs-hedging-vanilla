from utils.file_utils import summarize_to_xl, save_results
from utils.date_utils import get_prev_yr_from_hdg_dts, prev_bd
from HedgeModel.MktData.mkt_data import MktData
from datetime import datetime, date
from typing import Optional, Union
from pathlib import Path
import pandas as pd
from os import path
import datetime
import typing
import time
import os
from utils import assumption_loader_utils


# Move this to pandas_df_utils.py
def wghtd_avg(group_df: pd.DataFrame, whole_df: pd.DataFrame, values, weights):
    v = whole_df.loc[group_df.index, values]
    w = whole_df.loc[group_df.index, weights]
    return (v * w).sum() / w.sum()


class HedgeFile:

    __static_assum_fldr = os.path.join(os.getcwd(), 'Static_Assumptions')

    # Assum files as list (all csv files w/out the .csv so that names can be used as keys to dataframes containing their data!)
    __assum_files = ['HedgeDates', 'HdgFctrLU', 'CoPlanInd_to_Prod', 'Indicator_to_FundName', 'ProductDetailsByHedgeDate', 'Orion_IUL_Policies']

    # Add hedge_file_path Default location when available
    # NOTE:  This is the ULTIMATE LOCATION (PROD)
    # __hedge_file_fldr = r"\\rgare.net\STLCommon\ADMIN\NonTrad\CIMBA\CIMBA Prod\Anico\Imported Client Files"

    # These are TEMPORARY LOCATIONS  NOTE: SWITCH TO DEFAULT ABOVE WHEN CIMBA PROD HAS BEEN SETUP!!!
    # __hedge_file_fldr = "\\\\rgare.net\\stlcommon\\ADMIN\\NonTrad\\CIMBA\\CIMBA Prod\\Anico\\Received Client Files"
    __hedge_file_fldr = r"\\rgare.net\STLCommon\ADMIN\NonTrad\CIMBA\CIMBA Stage\Anico\Imported Client Files"


    # __base_output_path = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\RGA_Process\\2_Results'
    # __base_output_path = r'\\rgare.net\stlfinmkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\2_Results'
    __base_output_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Results'

    __status_added_date = date(2024, 10, 1)

    # MM_YYYY_HEDGE_ORIG eg) 11_2024_HEDGE_ORIG.txt
    # MM_YYYY _HEDGE_TRUE_UP eg) 11_2024_HEDGE_TRUE_UP.txt

    
    def __init__(self, hedge_file_path: Optional[Union[str, None]] = None, hedge_date: Optional[Union[str, datetime.date, None]] = None, is_true_up: Optional[Union[bool, None]] = None):
        """
        is_true_up -- Use False for Initial Hedge File, Use True for True-up Hedge File.  
        """
        self.hedgefile_df = self.inforce_summary_df = self.liab_summary_df = self.trading_summary_df = self.trading_summary_full_df = self.trading_summary_df_xl = self.updated_productdetails_df  = None
        self.inforce_summary_curr_mth_df = self.liab_summary_curr_mth_df = self.idx_credit_df = self.idx_credit_df_detailed_summary = None
        self.assum_dfs = self.hdgfctr_dict = self.output_path = None
        self.hedge_date = self.inforce_dt = self.first_bd = None
        self.hedge_file = None
        self.is_true_up = None

        # Setup By Reading Basic Assumptions
        self.setup(hedge_file_path, hedge_date, is_true_up)

    def setup(self, hedge_file_path: Optional[Union[str, None]] = None, hedge_date: Optional[Union[str, datetime.date, None]] = None, is_true_up: Optional[Union[bool, None]] = None):
        """
        Make sure to include validation of
        """
        # Need to load data for below in order to be able to conduct checks!
        self.assum_dfs = assumption_loader_utils.load_static_assumptions(assum_files=self.__assum_files)
        self.hdgfctr_dict = self.create_hdg_fctr_dict()

        hdgdts_df = self.assum_dfs['HedgeDates']

        # Conduct checks on hedge date and hedge file
        self.hedge_date = self.resolve_hedge_date(hedge_file_path, hedge_date)
        self.first_bd = hdgdts_df[hdgdts_df['HedgeDt']==self.hedge_date]['FirstBD'].values[0]
        self.is_true_up = self.resolve_is_true_up(hedge_file_path, is_true_up)
        self.hedge_file = self.resolve_hedge_file(hedge_file_path)

        true_up_str = 'True' if self.is_true_up else 'False'
        print(f'Hedge TrueUp is set to {true_up_str}')
        
    def resolve_hedge_date(self, hedge_file_path: Optional[Union[str, None]] = None, hedge_date: Optional[Union[str, datetime.date, None]] = None):
        """
        Cases:
        1) If hedge_date is not None, check if it is a valid hedge date.  If not, use assumed value from hedge dates tbl
        2) If hedge_date is None, if hedge_file is not None, try and extract date info from file name (check if valid after extracting)
        3) If hedge_date and hedge_file are both None, then lookup the hedge date based on the current day and month           
        """
        def parse_date(date_string, formats):
            """
            Parses a date string using a list of potential formats.

            Args:
                date_string: The string containing the date.
                formats: A list of date format strings.

            Returns:
            A datetime object if parsing is successful, otherwise None.
            """
            for fmt in formats:
                try:
                    return datetime.datetime.strptime(date_string, fmt)
                except ValueError:
                    continue

            print('Could not parse date from the string provided.  Defaulting the value to hedge date for the current month!')
            hedge_dt = datetime.datetime.today()
            hedge_dt = date(hedge_dt.year, hedge_dt.month, 1)
            
            return hedge_dt
               
        
        # First calc inforce date for each case, then update it to hedge date
        hedge_dt = None

        if hedge_date:
            
            if isinstance(hedge_date, datetime.date):
                                
                # Make sure the inforce date is the 1st of the month
                print('Using the hedge date provided in command line <date format>')
                hedge_dt = date(hedge_date.year, hedge_date.month, 1)
                
            elif isinstance(hedge_date, str):

                print('Parsing the hedge date provided in command line <string format>')
                
                dt_formats = ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d %b %Y", "%Y%m%d", "%Y-%m-%d", "%Y-%m-%D"]                
                hedge_dt = parse_date(hedge_date, dt_formats)

                # Make sure the inforce date is the 1st of the month
                hedge_dt = date(hedge_dt.year, hedge_dt.month, 1)
                                        
        else:
             
             if hedge_file_path:
                
                print('Attempting to extract date from hedge file since date parameter was not provided')
                # Should really check first if this is actually a file and not just a path
                hdg_file_name = os.path.basename(hedge_file_path)
                hdg_mth = int(hdg_file_name[:2])
                hdg_yr = int(hdg_file_name[3:7])

                try:
                    hedge_dt = date(hdg_yr, hdg_mth, 1)
                except ValueError:
                    print('Could not extract Date info from File Name -- setting the hedge date equal to the hedge date for the current month!')
                    hedge_dt = datetime.datetime.today()
                    hedge_dt = date(hedge_dt.year, hedge_dt.month, 1)
                             
             else:

                # Get the 1st of the month for this month
                print('Neither Hedge Date nor File Path Provided -- Defaulting to hedge date for the current month!')
                hedge_dt = datetime.datetime.today()
                hedge_dt = date(hedge_dt.year, hedge_dt.month, 1)

        # Now that we've got an inforce date defaulted to the 1st of the month in question, now lookup the true hedge date from the HedgeDates.csv tbl'
        
        
        # hdgdts_df = self.assum_dfs['HedgeDates']
        # hedge_date = hdgdts_df[hdgdts_df['InforceDt']==hedge_dt]['HedgeDate'].values[0]

        # self.inforce_dt = hedge_dt

        # print(f'Inforce Date set to {hedge_dt}')
        # print(f'Hedge Date set to {hedge_date}')
        
        # return hedge_date
        print(f'Hedge Date set to {hedge_dt}')
        return hedge_dt

    def resolve_is_true_up(self, hedge_file_path: Optional[Union[str, None]] = None, is_true_up: Optional[Union[bool, None]] = None):
        """
        To be called before file name has been resolved
        Cases:
        1) Provided w/ input params -- Just use value provided
        2) Derive from File Name
        3) Default to False if no filename or input param provided
        
        """
        if is_true_up:
            print('Using is_true_up flag provided in commmand line')
            return is_true_up
        else:

            if hedge_file_path:

                fname = str(os.path.basename(hedge_file_path))
                
                is_true_up = True if 'TRUE_UP' in fname else False                

                if is_true_up:
                    print('Based on the hedge file name, this is a true-up file')
                else:
                    print('TRUE_UP was not found in the file name and no cmd line arg was provided, so assuming this is not the true-up version of the file!')

                return is_true_up

            else:
                # Not enough info provided to determine, therefore return the default case of False, that this is the ORIG hedge file
                print('Not enough info provided to determine if this is the hedge TRUE_UP file, so assuming it is ORIG!')
                return False
            
    def resolve_hedge_file(self, hedge_file_path: Optional[Union[str, None]] = None):
        """
        Cases:
        1) If hedge_file is not None, use hedge_file, check if hedge_file_type is correct based on file_name convention.  If not, throw error, if so, set self.hedge_file_type. ELSE
        2) If hedge_file_type is not none, construct the filename based on hedge_date and hedge_file_type
        3) If both params are None, Send a warning that hedge_file is being set to latest hedge_file for the current day, based on the hedge_file_type == 0 <non-true-up file>
        """
        file = None

        def get_full_hedgefile_path(directory, is_true_up, hdg_dt):
            # Get hedge file for hedge date given 'is_true_up' and a root directory
            file_end = ('TRUE_UP' if is_true_up else 'ORIG') + '.txt'
            file_name = hdg_dt.strftime('%m_%Y') + '_HEDGE_' + file_end            
            full_file_path = os.path.join(directory, file_name)
            return full_file_path

        if hedge_file_path:
                            
            if os.path.isdir(hedge_file_path):
                # Shouldn't be the case, but a directory was passed rather than a file
                print(f'Hedge File Path provided is a directory!  Assuming the default file naming convention.')
                file = get_full_hedgefile_path(hedge_file_path, self.is_true_up, self.hedge_date)
            else:
                print(f'Hedge File Path provided as a non-directory for new hedges on {self.hedge_date}')
                file = hedge_file_path
                            
        else:
            # Get fully default file for hedge date given 'is_true_up'
            print ('No hedge file path provided in cmd line. Assuming default folder & file (based on other provided or defaulted params)')
            file = get_full_hedgefile_path(self.__hedge_file_fldr, self.is_true_up, self.hedge_date)

        # Double check just to make sure that the file we've constructed actually exists!
        if os.path.isfile(file):            
            print(f'Hedge File Path set to: {file}')
            return file
        else:
            raise Exception(f'The file: {os.path.basename(file)} was not present in the directory: {os.path.dirname(file)}')
            
    def run_all(self):
        # Load file, create summaries and save off results        
        self.load_hedgefile()
        self.create_summaries()                       
        self.save_all_results()

    def load_hedgefile(self):
        self.hedgefile_df = self.import_mthly_hedge_txtfile()
        # self.hedgefile_df = self.merge_dataframes(self.hedgefile_df)

    def create_summaries(self):
        # No Need to update product details, use 'mthly_product_details.py' for this instead!
        # self.updated_productdetails_df = self.create_product_details_df_for_new_hedge_dt()        
        self.inforce_summary_df = self.create_inforce_summary()
        self.liab_summary_df = self.create_liability_summary()
        self.trading_summary_df = self.create_trading_summary()

        if self.hedge_date >= date(2024, 7, 1):
            self.idx_credit_df = self.create_idx_credit_df()

    def create_output_fldr(self):
        
        # output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m%d'))
        output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m'))

        if not path.exists(output_path):
            os.makedirs(output_path)

        return output_path

    
    def get_budget_df(self):
        # Reduce the 'ProductDetailsByHedgeDate' DataFrame to only the join columns plus the budget
        # desired_cols = ['HedgeDate','Product_Detail','Indicator','Budget']
        desired_cols = ['HedgeDt','Product_Detail','Indicator','Budget']
        budget_df = self.assum_dfs['ProductDetailsByHedgeDate']
        budget_df = budget_df[desired_cols]
        return budget_df
    
    def get_hdg_fctr(self, hdg_dt: datetime.date, comp_id: int):
        """
        For Testing Purposes Only
        """
        if self.hdgfctr_dict is None:
            self.hdgfctr_dict=self.create_hdg_fctr_dict()
        
        return self.hdgfctr_dict[(hdg_dt, comp_id)]
        
        # Old code <Ugly>
        # hdg_fctr_df = self.assum_dfs['HdgFctrLU']
        # df_row = hdg_fctr_df[hdg_fctr_df['HedgeDate']==hdg_dt]
        # df_cols = [col_name + ('_NY' if comp_id == 26 else '') for col_name in ['HedgeRatio', 'Coins']]
        # return (df_row[df_cols[0]] * df_row[df_cols[1]]).values[0]
    
    def create_hdg_fctr_dict(self):
        
        hdg_fctr_df = self.assum_dfs['HdgFctrLU']
        
        # hdg_fctr_df.set_index('HedgeDate', inplace=True)
        hdg_fctr_df.set_index('HedgeDt', inplace=True)
        temp_dict = hdg_fctr_df.to_dict(orient='index')
        
        hdg_fctr_dict = {}

        for dt in temp_dict.keys():
            hdg_fctr_dict[(dt, 1)] = temp_dict[dt]['HedgeRatio'] * temp_dict[dt]['Coins']
            hdg_fctr_dict[(dt, 26)] = temp_dict[dt]['HedgeRatio_NY'] * temp_dict[dt]['Coins_NY']

        return hdg_fctr_dict

    def import_mthly_hedge_txtfile(self):

        print('Importing Hedge Txt File')
        start = time.time()
                        
        hedgefile_colnames = ['CompID', 'PolicyNum', 'Plan', 'IssueDate', 'Indicator', 'Tranx', 'Rev', 'Dept_Desk', 'Entry_Date', 'AsOf_Date', 'Base_Liab_Ntnl', 'Part', 'Cap', 'MGIR_(Cap)', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
                
        if self.hedge_date >= self.__status_added_date:
            hedgefile_colnames.append('Status')

        # Read hedge file
        hedgefile_df = pd.read_csv(self.hedge_file, index_col=False, header=None, names=hedgefile_colnames, skipfooter=2, engine='python', low_memory=True)

        

        # NOTE: Removing static addition of HedgeDate here and instead merging w/ HedgeDates.csv based on InforceDt == AsOf_Date'
        # Add HedgeDate to DataFrame
        # hedgefile_df['HedgeDate'] = self.hedge_date

        # Reminder to add HedgeDate to File and to date_cols below!
        # date_cols = ['HedgeDate', 'IssueDate', 'Entry_Date', 'AsOf_Date']
        date_cols = ['IssueDate', 'Entry_Date', 'AsOf_Date']
        for date_col in date_cols:
            hedgefile_df[date_col] = pd.to_datetime(hedgefile_df[date_col]).dt.date

        # Strip spaces out of 'PolicyNum', 'Plan', 'Tranx' and 'Indicator columns data
        hedgefile_df['PolicyNum'] = hedgefile_df['PolicyNum'].str.strip()
        hedgefile_df['Plan'] = hedgefile_df['Plan'].str.strip()
        hedgefile_df['Tranx'] = hedgefile_df['Tranx'].str.strip()
        hedgefile_df['Indicator'] = hedgefile_df['Indicator'].str.strip()

        # Adjust Fields to Divide by 100
        pct_fields = ['Floor', 'Spec_Rate', 'Spread', 'Part', 'Asset_Charge', 'Multiplier', 'Cap']
        for fld in pct_fields:
            hedgefile_df[fld] = hedgefile_df[fld] / 100.0
            hedgefile_df[fld] = hedgefile_df[fld].round(4)


        # Add status flag to make it consistent before/after
        if self.hedge_date < self.__status_added_date:
            hedgefile_df = self.add_orion_status_flag(hedgefile_df)
                
        end = time.time()
        exec_time = end - start
        print(f"Time Reading Hedge File: {int(exec_time // 60)} mins {int(exec_time % 60)} secs")

        self.hedgefile_df = hedgefile_df

        return hedgefile_df
    
    
    def add_orion_status_flag(self, df: pd.DataFrame):

        print('Adding Status field to DataFrame w/ value of N for Non-Orion policys and R for Resinured by RGA Orion policies!')
        
        orion_pols = self.assum_dfs['Orion_IUL_Policies']
        orion_pols['PolicyNum'] = orion_pols['PolicyNum'].str.strip()

        # Old Method
        # df = df.merge(orion_pols, on='PolicyNum')
        # New Method -- Create a mapping series from orion_pols
        orion_pols_series = orion_pols.set_index(['PolicyNum'])['Status']

        # Create a new Status column and preinit all values to 'N'
        df['Status'] = 'N'
        
        # df['Status'] = df.set_index(['PolicyNum']).index.map(orion_pols_series.get).fillna(df['Status'])
        # Now Update the 'Status' field to 'R' if PolicyNum was found in Orion Pols
        
        # Old
        # df.loc[df.set_index(['PolicyNum']).index.isin(orion_pols_series.index), 'Status'] = 'R'

                        
        # Copy the full tbl from loaded assumptions and set the index to the new index (enables update function)        
        df.set_index('PolicyNum', inplace=True)
        df.update(orion_pols_series)
        df.reset_index(inplace=True)
        
        return df
    
    
    def remove_non_orion(self, df: pd.DataFrame):
        
        # Filter by 'Status' containing 'R' for 'RGA' reinsured if the hedge date is past the status_added_date, otherwise join to the Orion_IUL_Policies table by PolicyNum        
        df = df[df['Status'].str.contains('R')]
        return df        
    
    def add_calculated_fields(self, df: pd.DataFrame):

        # Add Strike & Ntnl Multiplier to DataFrame
        df['Strike'] = 1 + df['Floor'] + df['Spread']
        df['Ntnl_Mult'] = df['Part'] * (1 + df['Multiplier'])

        # Add 'Adj_Liab_Ntnl' and 'Target_Liab_Ntnl' to DataFrame
        df['Target_Liab_Ntnl'] = df['HdgFctr'] * df['Base_Liab_Ntnl'] * df['Part'] * (1 + df['Multiplier'])        
        df['Adj_Liab_Ntnl'] = df['Target_Liab_Ntnl'] / df['HedgeRatio']

        return df

    def merge_dataframes(self, df: pd.DataFrame):

        print('Merging Hedge File w/ Helper Files to Add Additional Fields')
        start = time.time()

        # Merge in the HedgeDate here by joining AsOf_Dt to Inforce_Dt
        hdgdts_df = self.assum_dfs['HedgeDates']        
        # df = pd.merge(df, hdgdts_df, left_on='AsOf_Date', right_on='InforceDt', how='inner')
        df = pd.merge(df, hdgdts_df, left_on='AsOf_Date', right_on='HedgeDt', how='inner')
        df['HedgeDt'] = pd.to_datetime(df['HedgeDt']).dt.date
        # df['HedgeDate'] = pd.to_datetime(df['HedgeDate']).dt.date
        
        # Merge w/ co_to_hdgfctr to get hedge factors by company        
        # df = pd.merge(df, self.assum_dfs['HdgFctrLU'], on=['HedgeDate'])
        # df['HdgFctr'] = df.apply(lambda row: self.hdgfctr_dict[(row['HedgeDate'], row['CompID'])], axis=1)
        # df['HdgFctr'] = df.apply(lambda row:  row['HedgeRatio']*(row['Coins'] if row['CompID']==1 else row['Coins_NY']))

        # Get Distinct HedgeDts and Call a Function that Creates a HedgeFctrLuTbl
        # hdgfctr_lu_df = self.create_hdgfctr_lu_df(pd.DataFrame(inforce_df['HedgeDate'].unique(), columns=['HedgeDate']))
        # hdgfctr_lu_df = create_hdgfctr_lu_df(self.assum_dfs['HdgFctrLU'], pd.DataFrame(df['HedgeDate'].unique(), columns=['HedgeDate']))
        # df = df.merge(hdgfctr_lu_df, on=['HedgeDate', 'CompID'])
        hdgfctr_lu_df = assumption_loader_utils.create_hdgfctr_lu_df(self.assum_dfs['HdgFctrLU'], pd.DataFrame(df['HedgeDt'].unique(), columns=['HedgeDt']))
        df = df.merge(hdgfctr_lu_df, on=['HedgeDt', 'CompID'])
                         
        # Merge w/ coplanin_to_prod_df in order to add Product_Detail and Product to the DataFrame
        df = df.merge(self.assum_dfs['CoPlanInd_to_Prod'], on=['CompID','Plan', 'Indicator'])
        
        if 'Cap' in df.columns:
            df = df.merge(self.get_budget_df(), on=['HedgeDt', 'Product_Detail', 'Indicator'])
        else:
            df = df.merge(self.assum_dfs['ProductDetailsByHedgeDate'], on=['HedgeDt', 'Product_Detail', 'Indicator'])

        # Update Cap Field for Digital Options (Based on 'Indicator' == 'INXSPC')
        df['Cap'] = df.apply(lambda row: row['Spec_Rate'] if row['Indicator'] == 'INXSPC' else row['Cap'], axis=1)
        
        df['Budget'] = df['Budget'].astype(float)

        end = time.time()
        exec_time = end - start
        print(f"Finished...Time Merging Hedge File: {int(exec_time // 60)} mins {int(exec_time % 60)} secs")

        return df
    
    def create_idx_credit_df(self):

        if self.hedgefile_df is None:
            self.import_mthly_hedge_txtfile()

        print('Starting Creation of Index Crediting Summaries')
                
        df = self.hedgefile_df.copy(deep=True)
        
        # Create a list of surrendered policies
        surr_pols = df[df['Tranx']=='SURRENDER']['PolicyNum'].unique()

        # Get the list of policy records w/ interest credits
        credit_df = df[df['Tranx']=='EXCESS INT']

        # Filter out the policies that had surrender activity, then any remaining records where there was a 'Rev' <shouldn't be any, but just for safety>
        credit_df_no_surr = credit_df[~credit_df['PolicyNum'].isin(surr_pols)]
        credit_df_no_surr = credit_df_no_surr[credit_df_no_surr['Rev'].str.contains(' ')]
            
        # Get the hedge_dt and segment end date from 1-yr ago
        hdg_dts = self.assum_dfs['HedgeDates']
        prev_yr_hdg_dt = get_prev_yr_from_hdg_dts(hdg_dts, 'HedgeDt', self.hedge_date)
                
        # Update the 'AsOf_Date' field to be the value from 1-yr ago
        credit_df_no_surr['AsOf_Date'] = prev_yr_hdg_dt
                
        # Remove Cap & Other fields that would be incorrect since the idx credits are provided as going into the newly starting segment (as opposed to the maturing segment)
        # NOTE:  This is important in order to avoid duplicate cap, and other segment related data, as we need to join into the productdetails data to get the correct cap info from 1-yr ago
        cols_to_keep = ['CompID', 'PolicyNum', 'Plan', 'IssueDate', 'Indicator', 'Tranx', 'Rev', 'Dept_Desk', 'Entry_Date', 'AsOf_Date', 'Base_Liab_Ntnl', 'Status']
        credit_df_no_surr = credit_df_no_surr[cols_to_keep]

        # Add additional data from other tables
        credit_df_no_surr = self.merge_dataframes(credit_df_no_surr)

        # Merge in Fund Info
        credit_df_no_surr = credit_df_no_surr.merge(self.assum_dfs['Indicator_to_FundName'], on=['Indicator'])

        # Rename 'Base_Liab_Ntnl' to 'Idx_Credit_Amt'
        credit_df_no_surr.rename(columns={'Base_Liab_Ntnl': 'Idx_Credit_Amt'}, inplace=True)

        # Filter out any policies where Idx_Credit_Amt is NA or 0.0
        credit_df_no_surr = credit_df_no_surr[credit_df_no_surr['Idx_Credit_Amt'].notna()]
        credit_df_no_surr = credit_df_no_surr[credit_df_no_surr['Idx_Credit_Amt']!=0.0]

        # Add Strike & Ntnl Multiplier to DataFrame
        credit_df_no_surr['Strike'] = 1 + credit_df_no_surr['Floor'] + credit_df_no_surr['Spread']
        credit_df_no_surr['Ntnl_Mult'] = credit_df_no_surr['Part'] * (1 + credit_df_no_surr['Multiplier'])

        
        # Grab the unique indices and get the MktData object that stores the prices for these indices
        bbg_indices = list(credit_df_no_surr['Bbg_Idx'].unique())
        mkt_data = MktData(bbg_indices)

        # Start w/ assuming date for grabbing maturity price is the value for Seg_EndDt
        prev_yr_seg_startdt = hdg_dts[hdg_dts['HedgeDt']==prev_yr_hdg_dt]['Seg_StartDt'].values[0]
        prev_yr_seg_enddt = hdg_dts[hdg_dts['HedgeDt']==prev_yr_hdg_dt]['Seg_EndDt'].values[0]
        print(f'The prior year cohort was effective from {prev_yr_seg_startdt} and matures on {prev_yr_seg_enddt}, and the 1st BD of this month is {self.first_bd}')

        # Set the date to use for getting segment ending index level by default to the actual Seg_EndDt
        mkt_price_end_dt = prev_yr_seg_enddt
        
        # If we are processing the 'Orig' file, AND the prev_yr segment is ending today <the 1st BD of the month> then we need to get the mkt price on the last good BD
        if (prev_yr_seg_enddt == self.first_bd) & (self.is_true_up == False):                       
            mkt_price_end_dt = prev_bd(prev_yr_seg_enddt)
            print(f'Since this is the ORIG file AND the prior yr segment ends at the close of business today, need to grab mkt data for last good BD of {mkt_price_end_dt} instead of {prev_yr_seg_enddt}') 

        # Add a field containing the date used to obtain the mkt_price used in return used in determining index_credit
        credit_df_no_surr['IdxLvl_EndDt_DateUsed'] = mkt_price_end_dt

        # Reduce Down to Almost Final Columns
        semi_final_cols = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'IdxLvl_EndDt_DateUsed', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Tranx', 'Dept_Desk', 'Entry_Date', 'Product_Detail', 'Ntnl_Mult', 'Coins', 'HedgeRatio', 'HdgFctr', 'Budget', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Strike', 'Cap', 'Idx_Credit_Amt', 'Status']
        credit_df_no_surr = credit_df_no_surr[semi_final_cols]

        print(f'Grabbing Mkt Price Dict on {prev_yr_seg_startdt}')
        startdt_prices = mkt_data.get_px(prev_yr_seg_startdt)
        print(f'Prices on Prior Cohort Effective Date: {startdt_prices}')
        print(f'Grabbing Mkt Price Dict on {mkt_price_end_dt}')
        enddt_prices = mkt_data.get_px(mkt_price_end_dt)
        print(f'Prices for Prior Cohort Maturity, based on {mkt_price_end_dt}: {enddt_prices}')


        # Add the Index Price on the Seg_StartDt
        # credit_df_no_surr['IdxLvl_StartDt'] = credit_df_no_surr.apply(lambda row: mkt_data.get_px(row['Seg_StartDt'], row['Bbg_Idx']), axis=1)
        # credit_df_no_surr['IdxLvl_EndDt'] = credit_df_no_surr.apply(lambda row: mkt_data.get_px(mkt_price_end_dt, row['Bbg_Idx']), axis=1)
        credit_df_no_surr['IdxLvl_StartDt'] = credit_df_no_surr.apply(lambda row: startdt_prices[row['Bbg_Idx']], axis=1)
        credit_df_no_surr['IdxLvl_EndDt'] = credit_df_no_surr.apply(lambda row: enddt_prices[row['Bbg_Idx']], axis=1)
        credit_df_no_surr['Strike_Low'] = credit_df_no_surr['IdxLvl_StartDt'] * credit_df_no_surr['Strike']
        credit_df_no_surr['Strike_High'] = ''
        
        call_spread_criteria = (credit_df_no_surr['Opt_Type'] == 'Call Spread')
        credit_df_no_surr.loc[call_spread_criteria, 'Strike_High'] = credit_df_no_surr['IdxLvl_StartDt'] * (1 + credit_df_no_surr['Cap'])
        credit_df_no_surr['Idx_Rtn'] = credit_df_no_surr['IdxLvl_EndDt'] / credit_df_no_surr['IdxLvl_StartDt'] - 1

        def calc_payoff_pct_rtn(opt_type, px_start, px_end, cap, k_low, k_high):
                        
            if opt_type == 'Digital':                
                return cap if (px_end >= px_start) else 0.0
            
            lower_call_rtn = max((px_end - k_low) / px_start, 0.0)
            capped_rtn = 0

            if opt_type == 'Call Spread':
                capped_rtn = max((px_end - k_high) / px_start, 0.0)

            return lower_call_rtn - capped_rtn


        # Call the payoff function        
        credit_df_no_surr['Payoff_PctRtn'] = credit_df_no_surr.apply(lambda r: calc_payoff_pct_rtn(r['Opt_Type'], r['IdxLvl_StartDt'], r['IdxLvl_EndDt'], r['Cap'], r['Strike_Low'], r['Strike_High']), axis=1)
        
        # Calc Implied Ntnl (But only if payoff isn't 0 to avoid div by zero!)
        credit_df_no_surr['Implied_Ntnl'] = 0.0
        non_zero_payoff_criteria = (credit_df_no_surr['Payoff_PctRtn'] != 0.0)        
        credit_df_no_surr.loc[non_zero_payoff_criteria, 'Implied_Ntnl'] = credit_df_no_surr['Idx_Credit_Amt'] / credit_df_no_surr['Payoff_PctRtn']

        # Adjust the Implied Payoff Calc for when Indicator is INX150, since the credits coming through the file include credit for the floor of 1.5%
        non_zero_and_floor_criteria = (credit_df_no_surr['Payoff_PctRtn'] != 0.0) & (credit_df_no_surr['Indicator'] == 'INX150')
        # Strike is of the format 1.015, and what we want is just the 0.015 hence we subtact 1.0 from the strike below)        
        credit_df_no_surr.loc[non_zero_and_floor_criteria, 'Implied_Ntnl'] = credit_df_no_surr['Idx_Credit_Amt'] / (credit_df_no_surr['Payoff_PctRtn'] + credit_df_no_surr['Strike'] - 1.0)

                
        # Save policy level detail to dataframe
        # self.idx_credit_df = credit_df_no_surr.copy(deep=True)
        self.idx_credit_df = credit_df_no_surr

        # FOR PURPOSES OF RECON to ANICO -- INCLUDE STATUS FLAG AND COMPANY INFO
        group_by_cols_detailed = ['Status', 'HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'IdxLvl_EndDt_DateUsed', 'CompID', 'Fund_Name']
        # self.idx_credit_df_summary = (self.remove_non_orion(credit_df_no_surr)).groupby(group_by_cols, as_index=False).agg({'PolicyNum':'count', 'Idx_Credit_Amt':'sum', 'Implied_Ntnl':'sum'})
        
        # ORIG (updated to wghtd avg on 3/2/2025)
        # self.idx_credit_df_detailed_summary = credit_df_no_surr.groupby(group_by_cols_detailed, as_index=False).agg({'PolicyNum':'count', 'Idx_Credit_Amt':'sum', 'Implied_Ntnl':'sum'})

        # NEW (wghtd avg version)
        self.idx_credit_df_detailed_summary = credit_df_no_surr.groupby(group_by_cols_detailed, as_index=False).agg(
            {
                'PolicyNum': 'count',
                "Budget": lambda x: wghtd_avg(x, credit_df_no_surr, "Budget", "Implied_Ntnl"),
                "Strike": lambda x: wghtd_avg(x, credit_df_no_surr, "Strike", "Implied_Ntnl"),
                "Cap": lambda x: wghtd_avg(x, credit_df_no_surr, "Cap", "Implied_Ntnl"),                                                                
                'Idx_Credit_Amt':'sum',
                'Implied_Ntnl':'sum',
                'Idx_Rtn':'mean'
            }
        )

        self.idx_credit_df_detailed_summary.rename(columns={'PolicyNum': 'PolicyCount'}, inplace=True)
        self.idx_credit_df_detailed_summary['Segment_Idx_Credit_Rt'] = self.idx_credit_df_detailed_summary['Idx_Credit_Amt'] / self.idx_credit_df_detailed_summary['Implied_Ntnl']

        
        # CREATE THE SUMMARY TO BE USED IN COMPARISON WITH THE TRADING SUMMARY FROM PREVIOUS YEAR
        group_by_cols = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'IdxLvl_EndDt_DateUsed', 'Fund_Name']
        self.idx_credit_df_summary = self.idx_credit_df_detailed_summary.copy(deep=True)
        self.idx_credit_df_summary = self.idx_credit_df_summary[self.idx_credit_df_summary['Status']=='R']
        # self.idx_credit_df_summary = self.idx_credit_df_summary.groupby(group_by_cols, as_index=False).agg({'PolicyCount':'sum', 'Idx_Credit_Amt':'sum', 'Implied_Ntnl':'sum'})
        self.idx_credit_df_summary = self.idx_credit_df_summary.groupby(group_by_cols, as_index=False).agg(            
            {
                'PolicyCount': 'sum',
                "Budget": lambda x: wghtd_avg(x, self.idx_credit_df_summary, "Budget", "Implied_Ntnl"),
                "Strike": lambda x: wghtd_avg(x, self.idx_credit_df_summary, "Strike", "Implied_Ntnl"),
                "Cap": lambda x: wghtd_avg(x, self.idx_credit_df_summary, "Cap", "Implied_Ntnl"),
                'Idx_Credit_Amt':'sum',
                'Implied_Ntnl':'sum',                                                
                'Idx_Rtn':'mean'
                
            }
        )
        self.idx_credit_df_summary['Segment_Idx_Credit_Rt'] = self.idx_credit_df_summary['Idx_Credit_Amt'] / self.idx_credit_df_summary['Implied_Ntnl']

        return self.idx_credit_df

    def save_all_results(self):
        """
        Put the saving of files all in 1 place so it's easy to comment out any individual save
        """
        if self.output_path is None:
            self.output_path = self.create_output_fldr()
                
        xl_file_name = 'Orion_IUL_HedgeFile_Details_' + ('TrueUp' if self.is_true_up else 'Orig') + '.xlsx'
        xl_path = path.join(self.output_path, xl_file_name)

        # Define a dictionary of sheets and dataframes to output to Summary Workbook
        sheet_to_df_dict = {
            'Seriatim_All' : self.inforce_summary_df,
            'Seriatim_New_Cohort' : self.inforce_summary_curr_mth_df,
            'Summary_All' : self.liab_summary_df,
            'Summary_New_Cohort' : self.liab_summary_curr_mth_df,
            'Trading_Summary' : self.trading_summary_df_xl
        }

        if self.hedge_date >= date(2024, 7, 1):
            sheet_to_df_dict['Seriatim_Idx_Credits'] = self.idx_credit_df
            sheet_to_df_dict['Idx_Credit_ReconDetails'] = self.idx_credit_df_detailed_summary
            sheet_to_df_dict['Idx_Credit_Summary'] = self.idx_credit_df_summary

        # self.summarize_to_xl(xl_path, sheet_to_df_dict)
        summarize_to_xl(xl_path, sheet_to_df_dict)

        # NO LONGER NEED TO SAVE THIS TRADING SUMMARY!
        # save_results(self.output_path, 'iul_trading_summary' + self.true_up_outfile_suffix() + '.csv', self.trading_summary_df, True, 'Trade_Num')

        # region Old Save Method
        # self.save_results('iul_trading_summary' + self.true_up_outfile_suffix() + '.csv', self.trading_summary_df, True, 'Trade_Num')
        # self.save_results('iul_final_detail' + self.true_up_outfile_suffix() + '.xlsx', self.hedgefile_df, False)
        # self.save_results('iul_new_hedge_details' + self.true_up_outfile_suffix() + '.xlsx', self.inforce_summary_df, False)
        # self.save_results('iul_new_hedge_details_curr_mth' + self.true_up_outfile_suffix() + '.xlsx', self.inforce_summary_curr_mth_df, False)        
        # self.save_results('iul_liab_summary' + self.true_up_outfile_suffix() + '.csv', self.liab_summary_df, True, 'Liab_ID')
        # self.save_results('iul_liab_summary_curr_mth' + self.true_up_outfile_suffix() + '.csv', self.liab_summary_curr_mth_df, True, 'Liab_ID')
        # endregion Old Save Method


    def true_up_outfile_suffix(self):
        return '_TRUE_UP' if self.is_true_up else ''
    
    
    """ create_product_details -- No Longer Needed!  (Replaced w/ 'mthly_product_details.py')
    def create_product_details_df_for_new_hedge_dt(self):

        print('Creating required slice of table ProductDetailsByHedgeDate so that it can be referenced when inforce is loaded')

        # Get full file path to save updated tbl w/ extracted results to
        save_file = os.path.join(self.__static_assum_fldr, 'ProductDetailsByHedgeDate' + '.csv')

        # Setup a new index (to enable the production table to be updated w/ new data extracted from hedge file)
        new_idx = ['HedgeDate','Product_Detail','Indicator']
        
        # Old
        # desired_cols_plus_compID = ['CompID','HedgeDate','Product_Detail','Indicator','Budget','Part','Cap','Floor','Spec_Rate','Spread','Asset_Charge','Multiplier']
        # product_details_df = self.hedgefile_df[desired_cols_plus_compID].drop_duplicates()
                
        # Start w/ the inforce summary's desired output columns
        desired_output_cols = ['HedgeDate','Product_Detail','Indicator','Budget','Part','Cap','Floor','Spec_Rate','Spread','Asset_Charge','Multiplier']
        product_details_df = self.hedgefile_df[desired_output_cols].drop_duplicates()

        # Sort -- No Need
        # product_details_df.sort_values(by=['CompID', 'Product_Detail', 'Indicator'], inplace=True)
         # Remove 'CompID'
        # product_details_df = product_details_df[desired_output_cols]

        product_details_df.reset_index(drop=True, inplace=True)

        # Re-Index Summarized new data
        product_details_df.set_index(new_idx, inplace=True)

           
        # TODO: 1) Update the ProductDetails Table (use assumdf, filter out data for existing hedge date, merge new hedge data in, reapply sorting, then save results back to final table!)
        
        # Copy the full tbl from loaded assumptions and set the index to the new index (enables update function)
        full_details_tbl = self.assum_dfs['ProductDetailsByHedgeDate'].copy(deep=True)
        full_details_tbl.set_index(new_idx, inplace=True)

        
        # NOTE:  This assumes records are already present in the table for this hedge date.  This is handled by a separate process, sent when ANICO provides an update monthly table w/ budget info
        print(f'Updating data in {save_file}')
        full_details_tbl.update(product_details_df)

        # Reset the Index on the full table & sort
        full_details_tbl.sort_index(inplace=True)
        full_details_tbl.reset_index(inplace=True)

        # Sort again to be safe (index might sort by index num or something stupid)
        full_details_tbl.sort_values(by=new_idx, inplace=True)

        # Save results to file
        print(f'Saving results to {save_file}')
        full_details_tbl.to_csv(save_file, index=False)

        # Reset the summary dataframe ?
        # product_details_df.reset_index(inplace=True)
        
        # Return the result
        return product_details_df
    """

    def create_inforce_summary(self):
        """
        Used to create a summarized view of the monthly hedge or true-up file that can be joined 1-1 (by policynumber and indicator) to the full inforce file
        """
        print('Creating New Inforce Summary')
        """ Old groups
        # ordered_cols = ['HedgeDate', 'CompID', 'PolicyNum', 'Plan', 'Indicator', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier', 'Strike', 'Budget', 'HdgFctr', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl']
        # group_by_cols = ['HedgeDate', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier', 'Strike', 'Budget', 'HedgeRatio', 'HdgFctr']
        # group_by_cols = ['HedgeDate', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Cap', 'Strike', 'Budget', 'HedgeRatio', 'Ntnl_Mult']
        # group_by_cols = ['HedgeDate', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Cap', 'Strike', 'Budget', 'Ntnl_Mult', 'HedgeRatio']
        # ordered_cols = ['HedgeDate', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Cap', 'Strike', 'Budget', 'Ntnl_Mult', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        """
        group_by_cols = ['HedgeDt', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Cap', 'Strike', 'Budget', 'Ntnl_Mult', 'HedgeRatio']
        ordered_cols = ['HedgeDt', 'CompID', 'PolicyNum', 'IssueDate', 'Plan', 'Indicator', 'Cap', 'Strike', 'Budget', 'Ntnl_Mult', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']

        # Get the raw text file
        # hedgefile_df = self.hedgefile_df.copy(deep=True)

        # ----------APPLY FILTERS TO REDUCE THE DATAFRAME TO JUST RELEVANT NEW HEDGES --------------------------------------------------
        hedgefile_df = self.apply_liability_filters(self.hedgefile_df.copy(deep=True))
        # hedgefile_df = self.apply_liability_filters(hedgefile_df)
                
        # Merge in Required Columns From Other DataFrames
        hedgefile_df = self.merge_dataframes(hedgefile_df)

        # Add Calculated Fields
        hedgefile_df = self.add_calculated_fields(hedgefile_df)
        
        # Sum over the various 'Dept_Desk' for notional coming into an indicator for a given policynum
        hedgefile_inforce_summary_df = hedgefile_df.groupby(group_by_cols).agg({'Base_Liab_Ntnl': 'sum', 'Adj_Liab_Ntnl': 'sum', 'Target_Liab_Ntnl': 'sum'})
        hedgefile_inforce_summary_df.reset_index(inplace=True)

        hedgefile_inforce_summary_df = hedgefile_inforce_summary_df[ordered_cols]

        self.inforce_summary_df = hedgefile_inforce_summary_df
        self.inforce_summary_curr_mth_df = hedgefile_inforce_summary_df[hedgefile_inforce_summary_df['HedgeDt']==self.hedge_date]
        # self.inforce_summary_curr_mth_df = hedgefile_inforce_summary_df[hedgefile_inforce_summary_df['HedgeDate']==self.hedge_date]

        return hedgefile_inforce_summary_df
    

    def apply_liability_filters(self, df: pd.DataFrame):

        # Remove Non Orion Policies
        df = self.remove_non_orion(df)

        # Filter out 'Reversals' in 'Rev' column
        df = df[df['Rev'].str.contains(' ')]

        # Filter out 'Tranx' to only contain 'EXCHNGE TO'
        df = df[df['Tranx'].str.contains('EXCHNGE TO')]

        # Filter out any 0 or na Base Liab Ntnl
        df = df[df['Base_Liab_Ntnl']!=0]
        df = df[df['Base_Liab_Ntnl'].notna()]

        return df
   
    def create_liability_summary(self):

        print('Creating Summary of Liability')

        # Reduce the full dataframe to just necessary columns
        # required_cols = ['HedgeDate', 'CompID', 'Plan', 'Indicator', 'Product', 'Strike', 'Cap', 'Budget', 'Adj_Liab_Ntnl']
        # required_cols = ['HedgeDate', 'CompID', 'Plan', 'Indicator', 'Strike', 'Cap', 'Budget', 'PolicyNum', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        required_cols = ['HedgeDt', 'CompID', 'Plan', 'Indicator', 'Strike', 'Cap', 'Budget', 'PolicyNum', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        
        # Need to switch from 2 1ines below to 1 line below to get policy count correct!  (the grouping in inforce summary removes duplicate rows due to Tranx)
        hedgefile_df_reqcols = self.inforce_summary_df[required_cols]
        # hedgefile_df_reqcols = self.hedgefile_df[required_cols]
        
        # Summarize/Aggregate the simplified dataframe
        # initial_summary_df = hedgefile_df_reqcols.groupby(['HedgeDate', 'CompID', 'Indicator', 'Product', 'Strike', 'Cap', 'Budget']).agg({'Adj_Liab_Ntnl': 'sum'})
        # initial_summary_df = hedgefile_df_reqcols.groupby(['HedgeDate', 'CompID', 'Indicator', 'Strike', 'Cap', 'Budget', 'HedgeRatio']).agg({'PolicyNum': 'count', 'Base_Liab_Ntnl': 'sum', 'Adj_Liab_Ntnl': 'sum'})
        # initial_summary_df = hedgefile_df_reqcols.groupby(['HedgeDate', 'CompID', 'Indicator', 'Strike', 'Cap', 'Budget', 'HedgeRatio']).agg({'PolicyNum': 'count', 'Base_Liab_Ntnl': 'sum', 'Adj_Liab_Ntnl': 'sum', 'Target_Liab_Ntnl': 'sum'})
        initial_summary_df = hedgefile_df_reqcols.groupby(['HedgeDt', 'CompID', 'Indicator', 'Strike', 'Cap', 'Budget', 'HedgeRatio']).agg({'PolicyNum': 'count', 'Base_Liab_Ntnl': 'sum', 'Adj_Liab_Ntnl': 'sum', 'Target_Liab_Ntnl': 'sum'})
        initial_summary_df.reset_index(inplace=True)

        # Add results for FundName and Bbg Index
        liab_summary_df = initial_summary_df.merge(self.assum_dfs['Indicator_to_FundName'], on=['Indicator'])

        # Reorder and Reduce to the columns needed for the liability view, sort and reset
        # ordered_cols = ['HedgeDate', 'CompID', 'Indicator', 'Product', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Strike', 'Cap', 'Budget', 'Adj_Liab_Ntnl']
        # liab_summary_df.sort_values(by=['Fund_Name', 'CompID', 'Product'], inplace=True)
        # ordered_cols = ['HedgeDate', 'CompID', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Strike', 'Cap', 'Budget', 'HedgeRatio', 'PolicyNum', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl']
        # ordered_cols = ['HedgeDate', 'CompID', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Strike', 'Cap', 'Budget', 'PolicyNum', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        ordered_cols = ['HedgeDt', 'CompID', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Strike', 'Cap', 'Budget', 'PolicyNum', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        liab_summary_df = liab_summary_df[ordered_cols]        
        # liab_summary_df.sort_values(by=['HedgeDate', 'Fund_Name', 'CompID'], inplace=True)
        liab_summary_df.sort_values(by=['HedgeDt', 'Fund_Name', 'CompID'], inplace=True)
        liab_summary_df.reset_index(inplace=True, drop=True)

        # Add new column for Hedge Asset Pct
        # liab_summary_df['HedgeAssetPct'] = liab_summary_df['Adj_Liab_Ntnl'] / liab_summary_df.groupby(['HedgeDate', 'Indicator'])['Adj_Liab_Ntnl'].transform('sum')
        # liab_summary_df['HedgeAssetPct'] = liab_summary_df['Target_Liab_Ntnl'] / liab_summary_df.groupby(['HedgeDate', 'Indicator'])['Target_Liab_Ntnl'].transform('sum')
        liab_summary_df['HedgeAssetPct'] = liab_summary_df['Target_Liab_Ntnl'] / liab_summary_df.groupby(['HedgeDt', 'Indicator'])['Target_Liab_Ntnl'].transform('sum')

        # Rename Columns        
        # liab_summary_df.rename(columns={'Cap': 'Cap/Rate', 'PolicyNum': 'PolicyCount', 'Target_Liab_Ntnl': 'Notional'}, inplace=True)
        liab_summary_df.rename(columns={'Cap': 'Cap/Rate', 'PolicyNum': 'PolicyCount'}, inplace=True)

        # Save off dataframes
        self.liab_summary_df = liab_summary_df
        # self.liab_summary_curr_mth_df = liab_summary_df[liab_summary_df['HedgeDate']==self.hedge_date]
        self.liab_summary_curr_mth_df = liab_summary_df[liab_summary_df['HedgeDt']==self.hedge_date]
        self.liab_summary_curr_mth_df.reset_index(inplace=True, drop=True)
        
        return liab_summary_df

    def create_trading_summary(self):

        print('Creating Summary of Trading Ticket')


        
        
        # NOTE:  Consider restricting below to self.liab_summary_curr_mth_df
        # trading_summary_df = self.liab_summary_df.groupby(['HedgeDate', 'Fund_Name', 'Bbg_Idx', 'Opt_Type'], as_index=False).agg(
        trading_summary_df = self.liab_summary_df.groupby(['HedgeDt', 'Fund_Name', 'Bbg_Idx', 'Opt_Type'], as_index=False).agg(
            {
                'PolicyCount': 'sum',
                "Strike": lambda x: wghtd_avg(x, self.liab_summary_df, "Strike", "Target_Liab_Ntnl"),
                "Cap/Rate": lambda x: wghtd_avg(x, self.liab_summary_df, "Cap/Rate", "Target_Liab_Ntnl"),
                "Budget": lambda x: wghtd_avg(x, self.liab_summary_df, "Budget", "Target_Liab_Ntnl"),
                "Base_Liab_Ntnl": 'sum',
                "Adj_Liab_Ntnl": 'sum',
                "Target_Liab_Ntnl": 'sum'
            }
        )

        # Reset the Index
        trading_summary_df.reset_index(inplace=True, drop=True)

        # Store <but do not write> the full results <still grouped by hedge date
        self.trading_summary_full_df = trading_summary_df

        # Filter Down to just new trades w/ current effective date and reset the index
        # trading_summary_df = trading_summary_df[trading_summary_df['HedgeDate']==self.hedge_date]
        # trading_summary_df = trading_summary_df.loc[trading_summary_df['HedgeDate']==self.hedge_date]
        trading_summary_df = trading_summary_df.loc[trading_summary_df['HedgeDt']==self.hedge_date]
        trading_summary_df.reset_index(inplace=True, drop=True)


        
        # Get Segment EndDt
        hdgdts_df = self.assum_dfs['HedgeDates']        
        # df = pd.merge(df, hdgdts_df, left_on='AsOf_Date', right_on='InforceDt', how='inner')
        trading_summary_df = pd.merge(trading_summary_df, hdgdts_df, on=['HedgeDt'])
        # df['HedgeDt'] = pd.to_datetime(df['HedgeDt']).dt.date

        
        # MORE DETAILED OUTPUT TO EXCEL
        xl_cols = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'Fund_Name', 'PolicyCount', 'Bbg_Idx', 'Opt_Type', 'Strike', 'Cap/Rate', 'Budget', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'Target_Liab_Ntnl']                
        self.trading_summary_df_xl = trading_summary_df.copy(deep=True)
        self.trading_summary_df_xl = self.trading_summary_df_xl[xl_cols]

        # DEPRECATE THIS OUTPUT -- USE EXCEL INSTEAD        
        # cols = ['HedgeDate', 'Fund_Name', 'Bbg_Idx', 'Opt_Type', 'Strike', 'Cap/Rate', 'Budget', 'Base_Liab_Ntnl', 'Notional']
        cols = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'Fund_Name', 'PolicyCount', 'Bbg_Idx', 'Opt_Type', 'Strike', 'Cap/Rate', 'Budget', 'Base_Liab_Ntnl', 'Target_Liab_Ntnl']
        trading_summary_df = trading_summary_df[cols]
        trading_summary_df = trading_summary_df.rename(columns={'Target_Liab_Ntnl': 'Notional'})
        
                
        return trading_summary_df


if __name__ == "__main__":

    # NOTE:  Assumed inputs for hedge_file_input_path, hedge_date and target_hedge_fctr are in the process_mthly_hedge_file() function!!!
    # TODO:  Abstract the inputs so that entire process can be run from command line!
    pd.options.display.float_format = "{:,.2f}".format

    print("Starting Run:", datetime.datetime.now())
    start = time.time()

    # FOR TESTING!    
    # hedge_file = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\ANICO Files\\20241031\\Hedge Report\\HEDGE1104.txt'
    # hedge_date = datetime.date(2024,11,1)
    # hedge_file = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\ANICO Files\\20241130\\Hedge Report\\HEDGE1202.txt'
    # hedge_file = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\ANICO Files\\20241130\\Hedge Report\\12_2024_HEDGE_TRUE_UP.txt'
    # hedge_file = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\ANICO Files\\20241130\\Hedge Report\\12_2024_HEDGE_ORIG.txt'
    # hedge_date = datetime.date(2024,12,2)
    # hdg_file = HedgeFile()
    # hdg_file = HedgeFile(hedge_file_path=None, hedge_date=None, is_true_up=True)
    # hdg_file = HedgeFile(hedge_date='2024-11-01', is_true_up=True)


    # region Method 1 of looping code
    # runs = [('2024-11-01', False), ('2024-11-01', True), ('2024-12-01', False), ('2024-12-01', True)]

    # for run in runs:
    #     (hdg_dt, tu) = run
    #     hdg_file = HedgeFile(hedge_date=hdg_dt, is_true_up=tu)
    #     hdg_file.run_all()

    # endregion Method 1 of looping code

    # region Method 2 of looping code
    file_dir = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Archive'

    # for file_name in os.listdir(file_dir):
    # for file_name in ['08_2024_HEDGE_ORIG.txt', '09_2024_HEDGE_ORIG.txt', '10_2024_HEDGE_ORIG.txt']:
    # for file_name in ['09_2024_HEDGE_ORIG.txt', '10_2024_HEDGE_ORIG.txt']:
    # for file_name in ['01_2025_HEDGE_ORIG.txt', '01_2025_HEDGE_TRUE_UP.txt']:
    for file_name in os.listdir(file_dir):         
        # if file_name.upper() in ['01_2025_HEDGE_ORIG.TXT', '01_2025_HEDGE_TRUE_UP.TXT']:
        # if file_name.upper() in ['01_2025_HEDGE_ORIG.TXT']:
        # if file_name.upper() in ['02_2025_HEDGE_ORIG.TXT', '02_2025_HEDGE_TRUE_UP.TXT']:
        if file_name.upper() in ['04_2025_HEDGE_TRUE_UP.TXT']:
            file_path = os.path.join(file_dir, file_name)
            hdg_file = HedgeFile(hedge_file_path=file_path)
            hdg_file.run_all()


    # endregion Method 2 of looping code


    # hdg_file = HedgeFile(hedge_date='2024-11-01')
    # hdg_file.run_all()

    
    # process_mthly_hedge_file()

    end = time.time()
    elapsed_time = end - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    print("Finished Run:", datetime.datetime.now())
    print(f"Runtime: {minutes} mins {seconds} secs")
