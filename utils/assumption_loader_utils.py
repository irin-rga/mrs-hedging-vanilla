import os
import pandas as pd
import configparser

# Load configuration file
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))

# Get the current environment
current_env = config['ENV']['CURRENT_ENV']

def load_static_assumptions(assum_files=None, env=current_env):
    """
    Load static assumption data files into a dictionary of DataFrames.
    
    Parameters:
    env (str): The environment to use ('DEV' or 'PROD').
    
    Returns:
    dict: A dictionary where keys are file names and values are DataFrames.
    """
    assum_fldr = config[env]['ASSUM_FLDR']

    if assum_files is None:
        assum_files = ['HedgeDates', 'HdgFctrLU', 'CoPlanInd_to_Prod', 'Indicator_to_FundName', 'ProductDetailsByHedgeDate', 'Orion_IUL_Policies']
    
    print('Reading in Helper Dataframes (used for static lookups to obtain values for related fields)')
    print("REMINDER TO UPDATE NEW BUDGET INFO in Static_Assumptions Folder for ProductDetailsByHedgeDate.csv!!!")

    assum_dfs = {}

    for assum in assum_files:
        fname = os.path.join(assum_fldr, assum + '.csv')
        print(f'Reading Data from {fname}')
        assum_dfs[assum] = pd.read_csv(fname, index_col=False)
    
    # Make sure to update HedgeDate Column to a date!
    print('Converting Static Assum DataFrame Dates to Datetime.Date')

    if 'HedgeDates' in assum_files:
        assum_dfs['HedgeDates']['HedgeDt'] = pd.to_datetime(assum_dfs['HedgeDates']['HedgeDt']).dt.date
        assum_dfs['HedgeDates']['FirstBD'] = pd.to_datetime(assum_dfs['HedgeDates']['FirstBD']).dt.date
        assum_dfs['HedgeDates']['Seg_StartDt'] = pd.to_datetime(assum_dfs['HedgeDates']['Seg_StartDt']).dt.date
        assum_dfs['HedgeDates']['Seg_EndDt'] = pd.to_datetime(assum_dfs['HedgeDates']['Seg_EndDt']).dt.date
            
    if 'ProductDetailsByHedgeDate' in assum_files:
        assum_dfs['ProductDetailsByHedgeDate']['HedgeDt'] = pd.to_datetime(assum_dfs['ProductDetailsByHedgeDate']['HedgeDt']).dt.date

    return assum_dfs

def create_hdgfctr_lu_df(hdg_fctr_lu_df: pd.DataFrame, hdg_dts_df: pd.DataFrame):
    """
    hdg_dts_df:  A dataframe consisting of a single column of unique 'HedgeDate' values <column name must be 'HedgeDate'
    """
    hdg_fctr_tbl = hdg_fctr_lu_df.merge(hdg_dts_df, on=['HedgeDt'])
    desired_cols = ['HedgeDt', 'CompID', 'Coins', 'HedgeRatio', 'HdgFctr']

    hdg_fctr_non_ny_tbl = hdg_fctr_tbl.copy(deep=True)    
    hdg_fctr_non_ny_tbl['CompID'] = 1
    hdg_fctr_non_ny_tbl['Coins'] = hdg_fctr_tbl['Coins']
    hdg_fctr_non_ny_tbl['HdgFctr'] = hdg_fctr_tbl['HedgeRatio'] * hdg_fctr_tbl['Coins']
    hdg_fctr_non_ny_tbl = hdg_fctr_non_ny_tbl[desired_cols]

    hdg_fctr_ny_tbl = hdg_fctr_tbl.copy(deep=True)    
    hdg_fctr_ny_tbl['CompID'] = 26
    hdg_fctr_ny_tbl['Coins'] = hdg_fctr_tbl['Coins_NY']
    hdg_fctr_ny_tbl['HdgFctr'] = hdg_fctr_tbl['HedgeRatio_NY'] * hdg_fctr_tbl['Coins_NY']
    hdg_fctr_ny_tbl = hdg_fctr_ny_tbl[desired_cols]

    hdgfctr_lu_df = pd.concat([hdg_fctr_non_ny_tbl, hdg_fctr_ny_tbl], ignore_index=True)    

    return hdgfctr_lu_df
