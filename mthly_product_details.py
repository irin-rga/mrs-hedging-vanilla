from utils.file_utils import summarize_to_xl, save_results
from utils.assumption_loader_utils import load_static_assumptions
from datetime import datetime, date
from pandas import DataFrame as df
from typing import Optional, Union
from pathlib import Path
import pandas as pd
from os import path
import datetime
import time
import math
import os


class ProductDetails:

    # __static_assum_fldr = os.path.join(os.getcwd(), 'Static_Assumptions')
    __static_assum_fldr = r'C:\Users\S0053071\Repos\OrionWinterfell_Hedging\dev\Static_Assumptions'

    # Assum files as list (all csv files w/out the .csv so that names can be used as keys to dataframes containing their data!)
    # __assum_files = ['HedgeDates', 'ProductDetailsByHedgeDate']
    __assum_files = ['HedgeDates']

    # Add hedge_file_path Default location when available
    __new_product_details_dir = r"\\rgare.net\stlcommon\ADMIN\NonTrad\CIMBA\CIMBA Prod\Anico\Imported Client Files"

    __base_output_path = r'\\rgare.net\stlfinmkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\2_Results'
    # __base_output_path = 'C:/Users/S0053071/Repos/Orion_Process_Backup/RGA_Process/2_Results'

    
    #__example_file = r'C:\Users\S0053071\Repos\Orion_Process_Backup\12_2024_RGA_IUL_RATE_FEED.xlsx'

    
    def __init__(self, product_details_file_path: Optional[Union[str, None]] = None, product_details_file_date: Optional[Union[str, datetime.date, None]] = None):
        """
        hedge_file_type -- Use 0 for Initial Hedge File, Use 1 for True-up Hedge File.  
        """        
        self.assum_dfs = self.output_path = self.product_details_file =  None
        self.product_details_df = self.hdgdts_df =  None
        self.hedge_date = self.inforce_dt =  None

        # Setup By Reading Basic Assumptions -- NOTE:  Probably only need inforce_file_date now that naming convention is final
        self.setup(product_details_file_path, product_details_file_date)

        # Read and process file contents
        # self.product_details_df = self.import_product_details_file()

        # Save results

        
    def get_default_product_details_file_name(self):

        # self.inforce_dt should be set before calling this
        yr = str(self.inforce_dt.year)
        mth = self.inforce_dt.month
        mth = ('0' if mth < 10 else '') + str(mth)
        
        product_details_file_name = mth + '_' + yr + '_RGA_IUL_RATE_FEED.xlsx'

        return product_details_file_name
    
    def get_default_product_details_file(self):

        # self.inforce_dt should be set before calling this
        return os.path.join(self.__new_product_details_dir, self.get_default_product_details_file_name())
        
    def setup(self, product_details_file_path: Optional[Union[str, None]] = None, product_details_file_date: Optional[Union[str, datetime.date, None]] = None):
        """
        Make sure to include validation of
        """
        # Need to load data for below in order to be able to conduct checks!
        # self.assum_dfs = self.get_static_assum_dfs()
        # self.assum_dfs = get_static_assum_dfs(self.__static_assum_fldr, self.__assum_files)
        self.assum_dfs = load_static_assumptions(assum_files=self.__assum_files)

        hdgdts_df = self.assum_dfs['HedgeDates']
        self.hdgdts_df = hdgdts_df

        # Conduct checks on hedge date and hedge file
        self.inforce_dt = self.resolve_inforce_date(product_details_file_path, product_details_file_date)
        # self.hedge_date = hdgdts_df[hdgdts_df['InforceDt']==self.inforce_dt]['HedgeDate'].values[0]
        self.hedge_date = self.inforce_dt
        self.product_details_file = self.resolve_product_details_file(product_details_file_path)        
        
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

    def resolve_product_details_file(self, product_details_file_path: Optional[Union[str, None]] = None):
        """
        Cases:
        1) If inforce_file_path is not None, use inforce_file_path, check if hedge_file_type is correct based on file_name convention.  If not, throw error, if so, set self.hedge_file_type. ELSE
        2) If hedge_file_type is not none, construct the filename based on hedge_date and hedge_file_type
        3) If both params are None, Send a warning that hedge_file is being set to latest hedge_file for the current day, based on the hedge_file_type == 0 <non-true-up file>
        """

        file = None

        if product_details_file_path:

            if os.path.isfile(product_details_file_path):
                # Already a File
                print(f'Going on the assumption that product details data in {product_details_file_path} is consistent with the inforce date of {self.inforce_dt}')
                file = product_details_file_path
            elif os.path.isdir(path):
                # Shouldn't be the case, but a directory was passed rather than a file
                default_file_name =self.get_default_product_details_file_name()
                print(f'Going on the assumption that the default file {default_file_name} can be found for {self.inforce_dt} in the provided directory of: {product_details_file_path}')
                file = os.path.join(product_details_file_path, default_file_name)
            else:
                raise Exception('The inforce path provided was neither a Directory nor a File')
            
        else:
            default_inforce_file = self.get_default_product_details_file()
            print(f'Getting inforce data from the default path of {default_inforce_file}')
            file = default_inforce_file
                
        return file
         
    def run_all(self):
        # Load file, create summaries and save off results        
        self.product_details_df = self.import_product_details_file()                       
        self.save_all_results()
    
    def create_output_fldr(self):
        
        # output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m%d'))
        output_path = self.__static_assum_fldr

        if not path.exists(output_path):
            os.makedirs(output_path)

        return output_path
    
    def import_product_details_file(self):

        # Write and log file load start time
        print('Importing Product Details xlsx file...')
        start = time.time()
                                
        # Load the dataframe itself
        product_details_df = pd.read_excel(self.product_details_file)

        # Make sure HedgeDate is a Date!
        # product_details_df['HedgeDate'] = pd.to_datetime(product_details_df['HedgeDate']).dt.date
        product_details_df['HedgeDt'] = pd.to_datetime(product_details_df['HedgeDt']).dt.date
        
        # Add Inforce Date
        # product_details_df['InforceDt'] = date(product_details_df['HedgeDate'].year, product_details_df['HedgeDate'].month, 1)
        # product_details_df['InforceDt'] = product_details_df.apply(lambda r: date(r['HedgeDate'].year, r['HedgeDate'].month, 1), axis=1)

        # Make sure data fields are as expected
        product_details_df['Product_Detail'] = product_details_df['Product_Detail'].str.strip()
        product_details_df['Indicator'] = product_details_df['Indicator'].str.strip()
        product_details_df['Budget'] = product_details_df['Budget'].fillna(0)
        product_details_df['Budget'] = product_details_df['Budget'].round(4)
        product_details_df['Part'] = product_details_df['Part'].fillna(1)
        product_details_df['Cap'] = product_details_df['Cap'].fillna(9.9999)
        
        # Set NaN to 0 for the following fields
        for fld in ['Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']:
            product_details_df[fld] = product_details_df[fld].fillna(0)

        # Remove Hedge Date and HdgFctr from Fields
        # prelim_flds = ['InforceDt', 'Product_Detail', 'Indicator', 'Budget', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
        prelim_flds = ['HedgeDt', 'Product_Detail', 'Indicator', 'Budget', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
        product_details_df = product_details_df[prelim_flds]

        # Re-add the correct Hedge Date (will add Expiry Date as well)
        # product_details_df = product_details_df.merge(self.hdgdts_df, on=['InforceDt'])

        # Reduce down to final desired fields
        # final_flds = ['HedgeDate', 'Product_Detail', 'Indicator', 'Budget', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
        final_flds = ['HedgeDt', 'Product_Detail', 'Indicator', 'Budget', 'Part', 'Cap', 'Floor', 'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier']
        product_details_df = product_details_df[final_flds]

        end = time.time()
        exec_time = end - start
        print(f"Time Reading and Updating Product Details File: {int(exec_time // 60)} mins {int(exec_time % 60)} secs")

        return product_details_df
            

    def save_all_results(self):
        """
        Put the saving of files all in 1 place so it's easy to comment out any individual save
        """
        if self.output_path is None:
            self.output_path = self.create_output_fldr()
                
        self.save_results('ProductDetailsByHedgeDate.csv', self.product_details_df, False)
    
    def save_results(self, df_filename: str, df: pd.DataFrame, keep_idx: bool=False, idx_lbl: str=None):

        if self.output_path is None:
            self.output_path = self.create_output_fldr()
                
        print(f'Saving {df_filename} to: {self.output_path}')        
        
        full_path = path.join(self.output_path, df_filename)            
        
        save_start = time.time()

        if df_filename.endswith('.csv'):
            df.to_csv(full_path, index=keep_idx, index_label=idx_lbl)
        elif df_filename.endswith('.xlsx'):
            df.to_excel(full_path, index=keep_idx, index_label=idx_lbl)

        save_end = time.time()
        save_time = save_end - save_start
        print(f"Time spend saving {df_filename}: {int(save_time // 60)} mins {int(save_time % 60)} secs")
    

    
if __name__ == "__main__":

    # NOTE:  Assumed inputs for hedge_file_input_path, hedge_date and target_hedge_fctr are in the process_mthly_hedge_file() function!!!
    # TODO:  Abstract the inputs so that entire process can be run from command line!
    pd.options.display.float_format = "{:,.2f}".format
    pd.options.mode.copy_on_write = True

    print("Starting Run:", datetime.datetime.now())
    start = time.time()


    # product_details_file_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\12_2024_RGA_IUL_RATE_FEED.xlsx'
    product_details_file_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\ClientFiles_ForAssumCreation\03_2025_RGA_IUL_RATE_FEED.xlsx'

    # FOR TESTING!
    product_details = ProductDetails(product_details_file_path)
    product_details.run_all()
    
    end = time.time()
    elapsed_time = end - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    print("Finished Run:", datetime.datetime.now())
    print(f"Runtime: {minutes} mins {seconds} secs")
