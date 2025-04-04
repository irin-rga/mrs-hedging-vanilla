from utils.file_utils import summarize_to_xl, save_results
from datetime import datetime, date
from pandas import DataFrame as df
from typing import Optional, Union
from pathlib import Path
import pandas as pd
from os import path
import datetime
import time
import os
from utils import assumption_loader_utils


class InforceFile:

    __static_assum_fldr = os.path.join(os.getcwd(), 'Static_Assumptions')

    # Assum files as list (all csv files w/out the .csv so that names can be used as keys to dataframes containing their data!)
    __assum_files = ['HedgeDates', 'HdgFctrLU', 'CoPlanInd_to_Prod', 'FundID_to_Indicator', 'ProductDetailsByHedgeDate', 'Indicator_to_FundName', 'Inforce_FieldName_Map']

    # Add hedge_file_path Default location when available
    __inforce_file_path = "\\\\rgare.net\\stlcommon\\ADMIN\\NonTrad\\CIMBA\\CIMBA Prod\\Anico\\Imported Client Files"

    # __base_output_path = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\RGA_Process\\2_Results'
    __base_output_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Results'

    
    # MM_YYYY_HEDGE_ORIG eg) 11_2024_HEDGE_ORIG.txt
    # MM_YYYY _HEDGE_TRUE_UP eg) 11_2024_HEDGE_TRUE_UP.txt

    
    def __init__(self, inforce_file_path: Optional[Union[str, None]] = None, inforce_file_date: Optional[Union[str, datetime.date, None]] = None):
        """
        hedge_file_type -- Use 0 for Initial Hedge File, Use 1 for True-up Hedge File.  
        """
        self.inforce_df = self.raw_inforce_df = self.inforce_summary_df =  self.liab_summary_df = self.trading_summary_df = None
        self.int_credited_df = self.idx_credit_df_seriatim = None
        self.assum_dfs = self.output_path = self.new_hedge_file_path = self.new_hedge_df = None
        self.hedge_date = self.inforce_dt = self.inforce_file = None
        self.sheets_to_df_dict = {}
        
        
        # Setup By Reading Basic Assumptions -- NOTE:  Probably only need inforce_file_date now that naming convention is final
        self.setup(inforce_file_path, inforce_file_date)

        # self.inforce_df = self.import_inforce_txtfile()
        
        # Import the inforce file and associated new hedges
        # self.load_inforce_and_new_hedges()

        # TODO: Output Summary of Int_Credit and then filter out of core inforce_df
        # self.int_credited_df = ''

        # TODO: Filter out any data for the current Inforce_Dt <even though this may no longer be necessary> (verify this w/ Beau/Nick)

        # TODO: Appoend the current month's new hedge data and output the full liability file to file (to be used for attribution)

        # TODO: Summarize the full inforce data similar to the hedge txt file and output to file (to be used for daily valuations of liability)


                
    def get_default_inforce_file_name(self):

        # self.inforce_dt should be set before calling this
        yr = str(self.inforce_dt.year)
        mth = self.inforce_dt.month
        mth = ('0' if mth < 10 else '') + str(mth)
        
        inf_file_name = mth + '_' + yr + '_IUL_Fund_Values_RGA.txt'

        return inf_file_name
    
    def get_default_inforce_file(self):

        # self.inforce_dt should be set before calling this
        return os.path.join(self.__inforce_file_path, self.get_default_inforce_file_name())
    
    def setup(self, inforce_file_path: Optional[Union[str, None]] = None, inforce_file_date: Optional[Union[str, datetime.date, None]] = None):
        """
        Make sure to include validation of
        """
        # Need to load data for below in order to be able to conduct checks!
        self.assum_dfs = assumption_loader_utils.load_static_assumptions(assum_files=self.__assum_files)

        hdgdts_df = self.assum_dfs['HedgeDates']

        # Conduct checks on hedge date and hedge file
        self.inforce_dt = self.resolve_inforce_date(inforce_file_path, inforce_file_date)
        self.hedge_date = self.inforce_dt
        # self.hedge_date = hdgdts_df[hdgdts_df['InforceDt']==self.inforce_dt]['HedgeDate'].values[0]
        self.inforce_file = self.resolve_inforce_file(inforce_file_path)

        self.new_hedge_file_path = self.get_new_hedge_file_path()
        
    def resolve_inforce_date(self, inforce_file_path: Optional[Union[str, None]] = None, inforce_file_date: Optional[Union[str, datetime.date, None]] = None):
        """
        Cases:
        1) If inforce_file_date is not None, use it or throw error if it can't be parsed or isn't a date
        2) If inforce_file_date is None, if hedge_file is not None, try and extract date info from file name (check if valid after extracting)
        3) If inforce_file_date and hedge_file are both None, then lookup the hedge date based on the current day and month           
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
            return None
               
        inf_dt = None

        if inforce_file_date:
            
            if isinstance(inforce_file_date, datetime.date):
                                
                # Make sure the inforce date is the 1st of the month
                inf_dt = date(inforce_file_date.year, inforce_file_date.month, 1)
                
            elif isinstance(inforce_file_date, str):
                
                dt_formats = ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d %b %Y", "%Y%m%d", "%Y-%m-%d", "%Y-%m-%D"]                
                inf_dt = parse_date(inforce_file_date, dt_formats)

                # Make sure the inforce date is the 1st of the month
                inf_dt = date(inf_dt.year, inf_dt.month, 1)
                                        
        else:
             
             if inforce_file_path:

                # Should really check first if this is actually a file and not just a path
                inf_file_name = os.path.basename(inforce_file_path)
                hdg_mth = int(inf_file_name[:2])
                hdg_yr = int(inf_file_name[3:7])
                inf_dt = date(hdg_yr, hdg_mth, 1)
                return inf_dt
             
             else:

                # Get the 1st of the month for this month
                inf_dt = datetime.datetime.today()
                inf_dt = date(inf_dt.year, inf_dt.month, 1)

        return inf_dt

    def resolve_inforce_file(self, inforce_file_path: Optional[Union[str, None]] = None):
        """
        Cases:
        1) If inforce_file_path is not None, use inforce_file_path, check if hedge_file_type is correct based on file_name convention.  If not, throw error, if so, set self.hedge_file_type. ELSE
        2) If hedge_file_type is not none, construct the filename based on hedge_date and hedge_file_type
        3) If both params are None, Send a warning that hedge_file is being set to latest hedge_file for the current day, based on the hedge_file_type == 0 <non-true-up file>
        """

        file = None

        if inforce_file_path:

            if os.path.isfile(inforce_file_path):
                # Already a File
                print(f'Going on the assumption that inforce data in {inforce_file_path} is consistent with the inforce date of {self.inforce_dt}')
                file = inforce_file_path
            elif os.path.isdir(path):
                # Shouldn't be the case, but a directory was passed rather than a file
                default_file_name =self.get_default_inforce_file_name()
                print(f'Going on the assumption that the default file {default_file_name} can be found for {self.inforce_dt} in the provided directory of: {inforce_file_path}')
                file = os.path.join(inforce_file_path, default_file_name)
            else:
                raise Exception('The inforce path provided was neither a Directory nor a File')
            
        else:
            default_inforce_file = self.get_default_inforce_file()
            print(f'Getting inforce data from the default path of {default_inforce_file}')
            file = default_inforce_file
                
        return file
         
    def run_all(self):
        # Load file, create summaries and save off results        
        # self.load_inforce_and_new_hedges()
        self.create_inforce_and_idx_credits()
        # self.create_summaries()                       
        self.save_all_results()

    
    def create_output_fldr(self):
        
        # output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m%d'))
        output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m'))

        if not path.exists(output_path):
            os.makedirs(output_path)

        return output_path

    
    def get_budget_df(self):
        # Need all columns to populate cap/part/etc for inforce        
        budget_df = self.assum_dfs['ProductDetailsByHedgeDate']        
        return budget_df
        
    def create_inforce_and_idx_credits(self):
        self.inforce_df = self.import_inforce_txtfile()
        self.idx_credit_df = self.create_idx_credit_df()
    
    def import_inforce_txtfile(self):

        # Write and log file load start time
        print('Importing Inforce Txt File')
        start = time.time()
                                
        # Load the dataframe itself
        inforce_df = pd.read_csv(self.inforce_file, sep='\t')

        # Add Indicator field via FundID_to_Indicator Table
        inforce_df = inforce_df.merge(self.assum_dfs['FundID_to_Indicator'], on=['FND_ID_CD'])

        # Get Columns to Rename, Convert to Dictionary and rename the columns based on that dictionary
        col_rename_dict = { k : v for (k, v) in zip(self.assum_dfs['Inforce_FieldName_Map']['Orig_Name'], self.assum_dfs['Inforce_FieldName_Map']['New_Name'])}
        inforce_df.rename(columns=col_rename_dict, inplace=True)

        # Fix Field Format and Data Quality Issues.  1) Convert 'InforceDt' to Proper Date; 2) Restrict 'PolicyNum' to 1st 8 chars; 3) Strip spaces out of Plan
        # inforce_df['InforceDt']=pd.to_datetime(inforce_df.loc[:,'InforceDt']).dt.date
        inforce_df['HedgeDt']=pd.to_datetime(inforce_df.loc[:,'HedgeDt']).dt.date
        inforce_df['AsOfDt']=pd.to_datetime(inforce_df.loc[:,'AsOfDt']).dt.date
        inforce_df['PolicyNum'] = inforce_df.loc[:, 'PolicyNum'].str.slice(0, 8)
        inforce_df['Plan'] = inforce_df.loc[:, 'Plan'].str.strip()
        inforce_df['Status'] = inforce_df.loc[:, 'Status'].str.strip()        
        inforce_df = inforce_df[inforce_df['Status'].str.contains('R')]

        # Reduce the dataframe to only the columns we care about
        # inforce_cols_to_keep = ['InforceDt', 'AsOfDt', 'CompID', 'PolicyNum', 'PHA_Num', 'Plan', 'Indicator', 'Base_Liab_Ntnl', 'Idx_Credit']
        inforce_cols_to_keep = ['HedgeDt', 'AsOfDt', 'CompID', 'PolicyNum', 'PHA_Num', 'Plan', 'Indicator', 'Base_Liab_Ntnl', 'Idx_Credit']
        inforce_df = inforce_df[inforce_cols_to_keep]

        # Add the True HedgeDate and ExpirtyDt to the inforce file (Provided file assumes 'InforceDate' is the 1st actual day of the month and year of the HedgeDate)
        # inforce_df = inforce_df.merge(self.assum_dfs['HedgeDates'], on=['InforceDt'])
        inforce_df = inforce_df.merge(self.assum_dfs['HedgeDates'], on=['HedgeDt'])
                
        # Marge w/ 'CoPlanInd_to_Prod' on ['CompID', 'Plan', 'Indicator'] to get ['Product_Detail', 'Product']  <Note:  'Prod_Detail' used to join to 'ProductDetailsByHedgeDate' in subsequent step>
        inforce_df = inforce_df.merge(self.assum_dfs['CoPlanInd_to_Prod'], on=['CompID','Plan', 'Indicator'])

        # Marge w/ 'ProductDetailsByHedgeDate' on ['HedgeDate', 'Product_Detail', 'Indicator'] to get ['Budget', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
        # inforce_df = inforce_df.merge(self.get_budget_df(), on=['HedgeDate', 'Product_Detail', 'Indicator'])
        inforce_df = inforce_df.merge(self.get_budget_df(), on=['HedgeDt', 'Product_Detail', 'Indicator'])

        # Get Distinct HedgeDts and Call a Function that Creates a HedgeFctrLuTbl
        # hdgfctr_lu_df = self.create_hdgfctr_lu_df(pd.DataFrame(inforce_df['HedgeDate'].unique(), columns=['HedgeDate']))
        # hdgfctr_lu_df = create_hdgfctr_lu_df(self.assum_dfs['HdgFctrLU'], pd.DataFrame(inforce_df['HedgeDate'].unique(), columns=['HedgeDate']))
        # inforce_df = inforce_df.merge(hdgfctr_lu_df, on=['HedgeDate', 'CompID'])
        hdgfctr_lu_df = assumption_loader_utils.create_hdgfctr_lu_df(self.assum_dfs['HdgFctrLU'], pd.DataFrame(inforce_df['HedgeDt'].unique(), columns=['HedgeDt']))
        inforce_df = inforce_df.merge(hdgfctr_lu_df, on=['HedgeDt', 'CompID'])
        
        
        # NOTE: No need to divide fields by 100, this is taken care of in Hedge File Import (as reflected in the soon to be output caps, etc that are appended/updated in the 'ProductDetailsByHedgeDate.csv'
        # pct_fields = ['Floor', 'Spec_Rate', 'Spread', 'Part', 'Multiplier', 'Cap']
        # for fld in pct_fields:
        #     inforce_df[fld] = inforce_df[fld] / 100.0

        # Add Strike & Adj_Liab_Ntnl to DataFrame
        inforce_df['Strike'] = 1 + inforce_df['Floor'] + inforce_df['Spread']
        inforce_df['Ntnl_Mult'] = inforce_df['Part'] * (1 + inforce_df['Multiplier'])
        inforce_df['Target_Liab_Ntnl'] = inforce_df['HdgFctr'] * inforce_df['Base_Liab_Ntnl'] * inforce_df['Part'] * (1 + inforce_df['Multiplier'])
        # inforce_df['Adj_Liab_Ntnl'] = inforce_df['HdgFctr'] * inforce_df['Base_Liab_Ntnl'] * inforce_df['Part'] * (1 + inforce_df['Multiplier'])
        inforce_df['Adj_Liab_Ntnl'] = inforce_df['Target_Liab_Ntnl']  / inforce_df['HedgeRatio'] 


        # Update Cap Field for Digital Options (Based on 'Indicator' == 'INXSPC')
        inforce_df['Cap'] = inforce_df.apply(lambda row: row['Spec_Rate'] if row['Indicator'] == 'INXSPC' else row['Cap'], axis=1)

        # Save of Idx Credit DataFrame before filtering out the records not needed for constructing the inforce file!
        self.raw_inforce_df = inforce_df
        
        # TODO:
        # Filter out Records to only include where AsOfDt < InforceDt and Expiries >= HedgeDate
        # inforce_df = inforce_df[(inforce_df['AsOfDt'] < self.inforce_dt) & (inforce_df['ExpiryDt'] >= self.hedge_date)]
        inforce_df = inforce_df[(inforce_df['AsOfDt'] < self.inforce_dt) & (inforce_df['Seg_EndDt'] >= self.hedge_date)]

        # Filter out any 0 or na Base Liab Ntnl
        inforce_df = inforce_df[inforce_df['Base_Liab_Ntnl']!=0]
        inforce_df = inforce_df[inforce_df['Base_Liab_Ntnl'].notna()]


        end = time.time()
        exec_time = end - start
        print(f"Time Reading Inforce File: {int(exec_time // 60)} mins {int(exec_time % 60)} secs")


        # This is the list of all columns currently in the 'iul_new_inforce_details.xlsx' file (which we want to append to the bottom of the inforce)
        # inforce_cols_final = ['HedgeDate', 'CompID', 'PolicyNum', 'Plan', 'Indicator', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier', 'Strike', 'Budget', 'HdgFctr', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl']
        
        # # inforce_cols_final = ['CompID', 'PolicyNum', 'Plan', 'Indicator', 'Base_Liab_Ntnl', 'Part', 'Cap', 'MGIR_(Cap)', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
        # ordered_cols = ['InforceDt', 'HedgeDate', 'ExpiryDt', 'CompID', 'PolicyNum', 'Plan', 'Indicator', 'Base_Liab_Ntnl', 'Idx_Credit']
        # inforce_df = inforce_df[ordered_cols]


        return inforce_df
    
    def create_idx_credit_df(self):

        idx_credit_df = self.raw_inforce_df
        # idx_credit_eff_dt = idx_credit_df['HedgeDate'].min()
        # idx_credit_df = idx_credit_df[idx_credit_df['HedgeDate']==idx_credit_eff_dt]
        idx_credit_eff_dt = idx_credit_df['HedgeDt'].min()        
        idx_credit_df = idx_credit_df[idx_credit_df['HedgeDt']==idx_credit_eff_dt]
        idx_credit_df = idx_credit_df[idx_credit_df['Idx_Credit'].notna()]
        idx_credit_df['Idx_Credit_Net_Coins'] = idx_credit_df['Coins'] * idx_credit_df['Idx_Credit']
        
        
        idx_credit_df = idx_credit_df.merge(self.assum_dfs['Indicator_to_FundName'], on=['Indicator'])

        # group_by_cols = ['HedgeDate', 'ExpiryDt', 'CompID', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Budget', 'Ntnl_Mult', 'Cap', 'Strike']
        group_by_cols = ['HedgeDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Budget', 'Ntnl_Mult', 'Cap', 'Strike']
        # seriatim_cols = group_by_cols + ['AsOfDt', 'PolicyNum', 'Idx_Credit', 'Idx_Credit_Net_Coins']
        # seriatim_cols = group_by_cols.extend(['PolicyNum', 'Idx_Credit', 'Idx_Credit_Net_Coins'])

        # Save off seriatim results df
        # seriatim_cols_ordered = ['HedgeDate', 'AsOfDt', 'ExpiryDt', 'CompID', 'PolicyNum', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Budget', 'Ntnl_Mult', 'Cap', 'Strike', 'Idx_Credit', 'Idx_Credit_Net_Coins']
        seriatim_cols_ordered = ['HedgeDt', 'AsOfDt', 'Seg_EndDt', 'CompID', 'PolicyNum', 'Indicator', 'Bbg_Idx', 'Fund_Name', 'Opt_Type', 'Budget', 'Ntnl_Mult', 'Cap', 'Strike', 'Idx_Credit', 'Idx_Credit_Net_Coins']
        self.idx_credit_df_seriatim = idx_credit_df[seriatim_cols_ordered].copy(deep=True)

        # idx_credit_df = idx_credit_df.groupby(['HedgeDate', 'CompID', 'Indicator'], as_index=False).agg({'PolicyNum':'count', 'Base_Liab_Ntnl': 'sum', 'Adj_Liab_Ntnl': 'sum', 'Idx_Credit': 'sum'})
        idx_credit_df = idx_credit_df.groupby(group_by_cols, as_index=False).agg({'PolicyNum':'count', 'Idx_Credit': 'sum', 'Idx_Credit_Net_Coins': 'sum'})                        
        idx_credit_df.rename(columns={'PolicyNum': 'PolicyCount'}, inplace=True)
                    
        return idx_credit_df

    def get_seriatim_inforce_df(self):

        # inf_output_cols = ['HedgeDate', 'CompID', 'PolicyNum', 'AsOfDt', 'PHA_Num', 'Plan', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        inf_output_cols = ['HedgeDt', 'CompID', 'PolicyNum', 'AsOfDt', 'PHA_Num', 'Plan', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']
        
        # Reduce to just desired cols
        seriatim_inforce_df = self.inforce_df[inf_output_cols]

        # Remove inforce where Base_Liab_Ntnl is 'na' -- Already filtered out zero and na ntnl during import
        # seriatim_inforce_df = seriatim_inforce_df[seriatim_inforce_df['Base_Liab_Ntnl'].notna()]


        return seriatim_inforce_df

    def save_all_results(self):
        """
        Put the saving of files all in 1 place so it's easy to comment out any individual save
        """
        if self.output_path is None:
            self.output_path = self.create_output_fldr()

        # xl_file = 'Orion_IUL_Inforce_and_Index_Credits.xlsx'
        xl_file = 'Orion_IUL_Index_Credits (Inforce File).xlsx'
        
        xl_path = path.join(self.output_path, xl_file)

        # self.sheets_to_df_dict['Seriatim_Inforce'] = self.get_seriatim_inforce_df()
        self.sheets_to_df_dict['Idx_Credits_Seriatim'] = self.idx_credit_df_seriatim
        self.sheets_to_df_dict['Idx_Credits'] = self.idx_credit_df

        summarize_to_xl(xl_path, self.sheets_to_df_dict)
        save_results(self.output_path, 'Orion_IUL_Inforce_wOut_NewCohort.csv', self.get_seriatim_inforce_df(), False)

                
        # self.save_results('iul_new_inforce_details.xlsx', self.inforce_summary_df, False)
        # self.save_results('iul_liab_summary.csv', self.liab_summary_df, True, 'Liab_ID')

if __name__ == "__main__":

    # NOTE:  Assumed inputs for hedge_file_input_path, hedge_date and target_hedge_fctr are in the process_mthly_hedge_file() function!!!
    # TODO:  Abstract the inputs so that entire process can be run from command line!
    pd.options.display.float_format = "{:,.2f}".format
    pd.options.mode.copy_on_write = True

    print("Starting Run:", datetime.datetime.now())
    start = time.time()

    file_dir = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Archive'

    
    inforce_fldr = r'C:\Users\S0053071\Repos\Orion_Process_Backup\Inforce_Archive\v2' 
    
    for file_name in os.listdir(inforce_fldr):         
        # if file_name.upper() in ['01_2025_HEDGE_ORIG.TXT', '01_2025_HEDGE_TRUE_UP.TXT']:
        # if file_name not in ['12_2024_IUL_Fund_Values_RGA.txt']:
        if file_name in ['02_2025_IUL_Fund_Values_RGA.txt', '03_2025_IUL_Fund_Values_RGA.txt']:
            file_path = os.path.join(inforce_fldr, file_name)
            inf_file = InforceFile(inforce_file_path=file_path)
            inf_file.run_all()

    # FOR TESTING!
    # inforce_file_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\RGA_Process\12_2024_IUL_Fund_Values_RGA.txt'
    # inf_file = InforceFile(inforce_file_path)
    
    # hdg_file.run_all()

    
    # process_mthly_hedge_file()

    end = time.time()
    elapsed_time = end - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    print("Finished Run:", datetime.datetime.now())
    print(f"Runtime: {minutes} mins {seconds} secs")
