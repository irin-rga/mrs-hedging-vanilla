from datetime import datetime, date
from pathlib import Path
from os import path
import pandas as pd
from typing import List
import typing
import os

from crossasset.crossasset_base import CrossAssetContext
from nxpy import pro

conventions_file = Path(Path(__file__).parents[1], "Static_Assumptions", "USMktConventions.nxt")



def get_value_one_row_up(df, date_column, target_date):
    """Gets the value one row up from a given date in a dataframe column.

    Args:
        df: The DataFrame containing the data.
        date_column: The name of the column containing dates.
        target_date: The date to search for.

    Returns:
        The value one row up from the target date, or None if not found.
    """

    # Find the index of the target date
    index = df[df[date_column] == target_date].index

    # Check if the date was found
    if index.empty:
        return None

    # Check if the target date is in the first row
    if index[0] == 0:
        return None

    # Return the value from the previous row
    # return df.at[index[0] - 1, df.columns[1]]  # Assuming the value you want is in the second column
    return df.at[index[0] - 1, date_column]  # Assuming the value you want is in the second column

def get_value_offset_by_x_rows(df, date_column, target_date, x):
    """Gets the value one row up from a given date in a dataframe column.

    Args:
        df: The DataFrame containing the data.
        date_column: The name of the column containing dates.
        target_date: The date to search for.

    Returns:
        The value one row up from the target date, or None if not found.
    """

    # Find the index of the target date
    index = df[df[date_column] == target_date].index

    # Check if the date was found
    if index.empty:
        return None

    # Check if the target date is in the first row
    if index[0] == 0:
        return None

    # Return the value from the previous row
    # return df.at[index[0] - 1, df.columns[1]]  # Assuming the value you want is in the second column
    return df.at[index[0] + x, date_column]  # Assuming the value you want is in the second column

def get_prev_yr_from_hdg_dts(hdg_dts, date_column, tgt_dt):

    # Find the index of the target date
    index = hdg_dts[hdg_dts[date_column] == tgt_dt].index

    if (index.empty) or (index[0] < 12):
        return None
    
    return hdg_dts.at[index[0]-12, date_column]

def convert_to_date(cell_value):
    """Converts a cell value to date, handling NaT."""
    if pd.isna(cell_value):
        return pd.NaT
    return pd.to_datetime(cell_value).date()


""" Old get_hedge_date
def get_hedge_date(val_dt: date, hdgdts_df: pd.DataFrame):

    # 1st calc the 'inforce date' which can be used to easily look up the correct hedge date
    inf_dt = date(val_dt.year, val_dt.month, 1)

    # Now look up the hedge date
    hedge_date = hdgdts_df.loc[hdgdts_df['InforceDt']==inf_dt]['HedgeDate'].values[0]

    if val_dt < hedge_date:
        # Valuation Date is before the inforce file was received for this month.  Get last months inforce! 
        print(f'ValDt of {val_dt} is prior to the hedge effective date for the month <no new inforce yet>.  Grabbing inforce from previous month!')        
        hedge_date = get_value_one_row_up(hdgdts_df, 'HedgeDate', hedge_date)
        print(f'Attempting to grab inforce received on {hedge_date}')
        # In this case, the true-up should be true        
    
    return hedge_date
"""
def get_first_bd(val_dt: date, hdgdts_df: pd.DataFrame = None):

    # 1st calc the 'inforce date' which can be used to easily look up the correct hedge date
    hdg_dt = date(val_dt.year, val_dt.month, 1)

    # Now look up the hedge date
    first_bd = hdgdts_df.loc[hdgdts_df['HedgeDt']==hdg_dt]['FirstBD'].values[0]
    
    return first_bd

    # second_bd = next_bd(first_bd)

    # if val_dt < second_bd:
    #     # Valuation Date is before the inforce file was received for this month.  Get last months inforce! 
    #     print(f'ValDt of {val_dt} is prior to the 2nd business day for the month <no new inforce yet>.  Grabbing inforce from previous month!')        
    #     first_bd = get_value_one_row_up(hdgdts_df, 'FirstBD', first_bd)
    #     print(f'Attempting to grab inforce received on {first_bd}')
     
    

def get_second_bd(val_dt: date, hdgdts_df: pd.DataFrame = None):
    """
    Note:  Both the TRUE_UP file and the INFORCE file are now setup to arrive on the 2nd BD.  So we must use prior month INFORCE and either(Last Mth True-Up, Curr Mth Orig) in this case
    """    
    # Get the 1st Business Day
    first_bd = get_first_bd(val_dt, hdgdts_df)
    # Now get the 2nd bd
    second_bd = next_bd(first_bd)
    # return the 2nd bd
    return second_bd



def get_full_seriatim_inforce_file(inforce_fldr: str, val_dt: date, hdgdts_df: pd.DataFrame, save_results: bool = True):
    """
    Note -- Both the TRUE_UP file and the INFORCE file are now setup to arrive on the 2nd BD.  So we must use prior month INFORCE and either(Last Mth True-Up, Curr Mth Orig) in this case
    """
    
    # print(f'Calling get_full_seriatim_inforce_file on {val_dt}')
       
    full_seriatim_inf_file_name_root = 'Orion_IUL_Inforce_'
    inf_file_name = 'Orion_IUL_Inforce_wOut_NewCohort.csv'
    hdg_file_name_root = 'Orion_IUL_HedgeFile_Details_' #Orig or TrueUp + .xlsx

    # Determine the 1st and 2nd business days date based on the valuation date
    first_bd = get_first_bd(val_dt, hdgdts_df)
    second_bd = get_second_bd(val_dt, hdgdts_df)

    # Create a date converter for pandas to read date cols in excel as dates
    date_cols = ['HedgeDt']
    converters = {col: convert_to_date for col in date_cols}
    
    # Initialize prior mth fldr variables here so we can access them later in conditional statements
    prior_hdg_mth = prior_hdg_yr = prior_hdg_dt = prev_mth_fldr = None

    # These defaults are to current month (Case True-up) but reverts to prior month if we haven't received new inforce yet <before 2nd bd>
    mth_fldr = first_bd.strftime('%Y%m')
    true_up = True

    # Need to get this info in case the inforce file should be retreived from prior mth folder
    if val_dt < second_bd:
        # We receive the 'Orig' file on Day 1, so this should be the latest        
        # Get the prior month folder info
        prior_hdg_mth = 12 if val_dt.month == 1 else val_dt.month - 1
        prior_hdg_yr = val_dt.year - 1 if prior_hdg_mth == 12 else val_dt.year
        prior_hdg_dt = date(prior_hdg_yr, prior_hdg_mth, 1)
        prev_mth_fldr = path.join(inforce_fldr, prior_hdg_dt.strftime('%Y%m'))
        # Override true-up from default case, since we need prior mth inforce plus plus prior month true-up plus curr mth 'Orig'
        # mth_fldr = prev_mth_fldr
        true_up = False
        
    # Set true-up string
    true_up_str = 'TrueUp' if true_up else 'Orig'

    # FIRST, WE WANT TO CHECK FOR A FINISHED, FULL INFORCE FILE!
    full_seriatim_inf_file_name = full_seriatim_inf_file_name_root + true_up_str + '.csv'
    full_seriatim_path = path.join(inforce_fldr, mth_fldr, full_seriatim_inf_file_name)

    if os.path.exists(full_seriatim_path):
        # Full Seriatim File Found.  Now read it in and return the dataframe!
        print(f'Returning data from {full_seriatim_path}')
        full_inforce_df = pd.read_csv(full_seriatim_path)
        return full_inforce_df
        # return pd.read_csv(full_seriatim_path)
    else:

        # Full Seriatim File Not Found.  Now try and create it!

        # 2 Cases:  
        # 1) if val_dt < 2nd BD: use Prev Inforce + Prev True-up + Curr Orig
        # 2) otherwise: use Curr Inforce + Curr True-up

        def all_files_exist(file_paths): 
        # Takes a list of strings containing file paths and checks that they all exist, otherwise prints the names of the paths not found.
        # Returns True if all files found, else false

            if all([os.path.isfile(f) for f in file_paths]):
                return True
            else:
                print('The following file(s) could not be found!:')

                for f in file_paths:
                    if not os.path.exists(f):
                        print(f'   {f}')

                return False


        full_inforce_df = None

        if val_dt < second_bd:           
            # Get paths for the prev inforce, prev true-up and curr 'Orig' files
            prev_inf_file = path.join(prev_mth_fldr, inf_file_name)
            prev_hdg_file = path.join(prev_mth_fldr, hdg_file_name_root + 'TrueUp.xlsx')
            curr_hdg_file = path.join(inforce_fldr, val_dt.strftime('%Y%m'), hdg_file_name_root + 'Orig.xlsx')

            # Make sure the files exist!
            if not all_files_exist([prev_inf_file, prev_hdg_file, curr_hdg_file]):
                return
            
            # Show msg about what inforce is being created
            print(f'Creating full seriatim inforce from the Previous Month {prior_hdg_dt.strftime('%Y_%b')} Inforce and TrueUp HedgeFile plus the ORIG file for the {val_dt.strftime('%Y_%b')}!')

            # 1st Combine the Previous Inforce and True-Up
            prev_inf_df = pd.read_csv(prev_inf_file)
            prev_hdg_df = pd.read_excel(prev_hdg_file, sheet_name='Seriatim_New_Cohort', converters=converters)
            previous_combined_df = combine_inforce_and_new_hedges(prev_inf_df, prev_hdg_df)

            # Next combine the result from above w/ the new Hedge 'ORIG' txt file
            curr_hdg_df = pd.read_excel(curr_hdg_file, sheet_name='Seriatim_New_Cohort', converters=converters)
            full_inforce_df = combine_inforce_and_new_hedges(previous_combined_df, curr_hdg_df)

        else:

            # 1st we need to know which version of hedge txt file to merge on top of the Inforce
            hdg_file_name = hdg_file_name_root + true_up_str + '.xlsx'

            # Now we need the paths of both the hedge file and inforce file to check for their existinence and load them if they do
            inf_file_path = path.join(inforce_fldr, first_bd.strftime('%Y%m'), inf_file_name)        
            hdg_file_path = path.join(inforce_fldr, first_bd.strftime('%Y%m'), hdg_file_name)

            # Make sure the files exist!
            if not all_files_exist([inf_file_path, hdg_file_path]):
                return
            
            # Show msg about what inforce is being created
            print(f'Creating full seriatim inforce using the {val_dt.strftime('%Y_%b')} versions of the {inf_file_name} and {true_up_str}!')
            curr_inf_df = pd.read_csv(inf_file_path)
            curr_hdg_df = pd.read_excel(hdg_file_path, sheet_name='Seriatim_New_Cohort', converters=converters)
            full_inforce_df = combine_inforce_and_new_hedges(curr_inf_df, curr_hdg_df)
            

        if save_results:
                # Save combined df
                print(f'Saving full seriatim file to {full_seriatim_path}')
                full_inforce_df.to_csv(full_seriatim_path, index=False)

        # Return the newly created df
        return full_inforce_df
            
            
# def combine_inforce_and_new_hedges(inf_file_path, hdg_file_path) -> pd.DataFrame:
#     # Read in dataframes
#     inf_df = pd.read_csv(inf_file_path)
#     hdg_df = pd.read_excel(hdg_file_path, sheet_name='Seriatim_New_Cohort')

def combine_inforce_and_new_hedges(inf_df, hdg_df) -> pd.DataFrame:

    # Set the common columns to use
    desired_cols = ['HedgeDt', 'CompID', 'PolicyNum', 'Plan', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'HedgeRatio', 'Target_Liab_Ntnl']

    # Reduce to identical columns
    inf_df = inf_df[desired_cols]
    hdg_df = hdg_df[desired_cols]

    # Eliminate any cases where Base_Liab_Ntnl is zero
    inf_df = inf_df[inf_df['Base_Liab_Ntnl']!=0]
    inf_df = inf_df[inf_df['Base_Liab_Ntnl'].notna()]
    hdg_df = hdg_df[hdg_df['Base_Liab_Ntnl']!=0]
    hdg_df = hdg_df[hdg_df['Base_Liab_Ntnl'].notna()]
    
    # Combine them
    seriatim_inf_df = pd.concat([inf_df, hdg_df], ignore_index=True)
    seriatim_inf_df['HedgeDt']=pd.to_datetime(seriatim_inf_df.loc[:,'HedgeDt']).dt.date

    # Sort the values and reset the index
    seriatim_inf_df.sort_values(by=['HedgeDt', 'CompID', 'Indicator', 'Plan'], inplace=True)
    seriatim_inf_df.reset_index(inplace=True, drop=True)

    # Return the combined seriatim dataframe
    return seriatim_inf_df


# region Nx Date Utils

def add_tenor(start_dt: date, tenor: str, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

    app = pro.Application()
    warning = pro.ApplicationWarning()
    # CrossAssetContext(app)

    # Read calendars and conventions
    app.read_nxt(conventions_file, warning)

    return app.add_tenor(start_dt, tenor, conv, cal, warning, eom)
    
def sub_tenor(start_dt: date, tenor: str, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

    app = pro.Application()
    warning = pro.ApplicationWarning()
    # CrossAssetContext(app)

    # Read calendars and conventions
    app.read_nxt(conventions_file, warning)

    return app.sub_tenor(start_dt, tenor, conv, cal, warning, eom)

def next_bd(start_dt: date, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):
    return add_tenor(start_dt, '1BD', conv, cal, eom)

def prev_bd(start_dt: date, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):
    return sub_tenor(start_dt, '1BD', conv, cal, eom)

def get_maturity(start_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'P', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):
                
    tnr_yrs = str(num_of_yrs) + 'Y'
    return add_tenor(start_dt, tnr_yrs, conv, cal, eom)

# endregion Nx Date Utils

# region 'Winterfell 1) Start\Effective from Expiry, and; 2) Expiry from Start\Effective Date Helper Functions

def third_friday(year, month):
        """Return datetime.date for monthly option expiration given year and
        month
        """
        # The 15th is the lowest third day in the month
        third = datetime.date(year, month, 15)
        # What day of the week is the 15th?
        w = third.weekday()
        # Friday is weekday 4
        if w != 4:
            # Replace just the day (of month)
            third = third.replace(day=(15 + (4 - w) % 7))
        return third

def get_Winterfell_IUL_ExpiryDt(start_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

    """
    NOTE -- For Winterfell (IUL):
    Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
    Segment Maturity Date:  The 1st BD occuring after the 13th calendar day of the month
    """
    maturity_dt = start_dt + pd.DateOffset(years=num_of_yrs)
    maturity_dt = maturity_dt.replace(day=13)

    maturity_dt = add_tenor(maturity_dt, "1BD", conv, cal, eom)
    return maturity_dt

def get_Winterfell_IUL_StartDt_From_ExpiryDt(maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

    """
    NOTE -- For Winterfell (IUL):
    Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
    Segment Maturity Date:  The 1st BD occuring after the 13th calendar day of the month
    """
    start_dt = maturity_dt + pd.DateOffset(years=-1*num_of_yrs)
    start_dt = start_dt.replace(day=13)

    start_dt = add_tenor(start_dt, "2BD", conv, cal, eom)
    return start_dt

def get_Winterfell_VUL_ExpiryDt(start_dt: date, num_of_yrs: int):

    """
    NOTE -- For Winterfell (IUL):
    Segment Start Date: The 2nd BD occuring after the 13th calendar day of the month
    Segment Maturity Date:  The 1st BD occuring after the 13th calendar day of the month
    """
    yr = start_dt.year + num_of_yrs
    mth = start_dt.month

    maturity_dt = third_friday(yr, mth)
    return maturity_dt        

def get_Winterfell_VUL_StartDt_From_ExpiryDt(maturity_dt: date, num_of_yrs: int):

    """
    NOTE -- For Winterfell (VUL):
    Segment Start Date: 3rd Friday
    Segment Maturity Date:  3rd Friday
    """
    
    yr = maturity_dt.year - num_of_yrs
    mth = maturity_dt.month
    
    start_dt = third_friday(yr, mth)
    return start_dt

def get_Winterfell_StartDt_From_ExpiryDt(Product: str, maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

    if Product == 'IUL':
        return get_Winterfell_IUL_StartDt_From_ExpiryDt(maturity_dt, num_of_yrs, conv, cal, eom)
    else:
        return get_Winterfell_VUL_StartDt_From_ExpiryDt(maturity_dt, num_of_yrs)
    
def get_Winterfell_ExpiryDt(Product: str, maturity_dt: date, num_of_yrs: int, conv: typing.Optional[str] = 'F', cal: typing.Optional[str] = 'NewYork', eom: typing.Optional[bool]=False):

    if Product == 'IUL':
        return get_Winterfell_IUL_ExpiryDt(maturity_dt, num_of_yrs, conv, cal, eom)
    else:
        return get_Winterfell_VUL_ExpiryDt(maturity_dt, num_of_yrs)

# endregion 'Winterfell 1) Start\Effective from Expiry, and; 2) Expiry from Start\Effective Date Helper Functions

    

