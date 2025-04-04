from utils.date_utils import get_second_bd, prev_bd, get_full_seriatim_inforce_file, get_first_bd, get_second_bd, next_bd
from utils.file_utils import summarize_to_xl, save_results, read_excel_df_with_dates
from utils.decoration_utils import timing, timer
from HedgeModel.MktData.mkt_data import MktData
from HedgeModel.positions import Position
from typing import Optional, Union, List
from datetime import datetime, date
from pathlib import Path
import pandas as pd
from os import path
import numpy as np
import typing
import time
import os
from utils.assumption_loader_utils import load_static_assumptions


class OrionAsset:

    # __static_assum_fldr = os.path.join(os.getcwd(), 'Static_Assumptions')
    # __static_assum_fldr = os.path.join(Path().resolve().parents[0], 'Static_Assumptions')
    # __static_assum_fldr = os.path.join(Path().resolve().parents[0], 'Static_Assumptions')
    # __static_assum_fldr = r'\\rgare.net\stlfinmkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\1_Code\Static_Assumptions'

    # Assum files as list (all csv files w/out the .csv so that names can be used as keys to dataframes containing their data!)
    __assum_files = ['HedgeDates']

    # __asset_holdings_file = r'\\rgare.net\StlFinMkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\Orion_HedgeAsset_Holdings.xlsx'
    __asset_holdings_file = r'C:\Users\S0053071\Repos\Orion_Process_Backup\Orion_HedgeAsset_Holdings.xlsx'

    __holdings_file_sheet_name = 'Orion_HedgeAsset_Holdings'
    
    # __base_output_path = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\RGA_Process\\2_Results'
    __base_output_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Results'

    
    def __init__(self, attrib_start_dt : Optional[Union[date, None]] = None, attrib_end_dt: Optional[Union[date, None]] = None, assum_dfs: Optional[dict] = None):
        """
        
        """        
        self.inforce_dt = self.first_bd = self.hedge_dt = self.second_bd =  None
        self.assum_dfs = self.output_path = None                
        self.is_true_up = None

        # Initialize Dates
        self.attrib_end_dt = self.attrib_start_dt = self.first_bd = None

        # Initialize Empty DataFrames          
        self.raw_holdings_df = None             
        self.position_attrib_df = None
                
        # Setup By Reading Basic Assumptions
        self.setup(attrib_start_dt, attrib_end_dt, assum_dfs)

    def setup(self, prev_dt, curr_dt, assum_dfs):
        """
        Make sure to include validation of
        """
        # Need to load data for below in order to be able to conduct checks!
        # self.assum_dfs = self.get_static_assum_dfs()
        if assum_dfs:
            self.assum_dfs = assum_dfs
        else:
            self.assum_dfs = load_static_assumptions(assum_files=self.__assum_files)
        
        self.attrib_end_dt = self.resolve_attrib_end_dt(curr_dt)
        self.attrib_start_dt = self.resolve_attrib_start_dt(prev_dt)

        self.first_bd = get_first_bd(self.attrib_end_dt, self.assum_dfs['HedgeDates'])
        self.second_bd = get_second_bd(self.attrib_end_dt, self.assum_dfs['HedgeDates'])
        self.hedge_dt = date((self.first_bd).year, self.first_bd.month, 1)
                
    def resolve_attrib_end_dt(self, attrib_end_dt):
        if attrib_end_dt is None:
            # Get today
            temp_dt = datetime.today()
            # Assume the desired 'curr_dt' for attribution is the previous bd from today, since we'd have full mkt data for that date
            return prev_bd(temp_dt)
        else:
            return attrib_end_dt
        
    def resolve_attrib_start_dt(self, attrib_start_dt):
        if attrib_start_dt is None:

            
            # Return the previous business day from today
            return prev_bd(self.attrib_end_dt)
        else:            
            if attrib_start_dt != prev_bd(self.attrib_end_dt):
                print('CAUTION:  The previous date provided for attribution purposes is not the previous business of the end/current date for attribution!')            
            return attrib_start_dt
        
    
    def update_attrib_dates(self, attrib_start_dt_new: date, attrib_end_dt_new: date):
        raise NotImplementedError

    def update_attrib_end_dt(self, attrib_end_dt_new: date):
        """
        Updates the 'curr_dt' which represents the end date of the daily attribution.  This will allow attribution data to be updated w/out reloading everything
        when unloading would be unnecessary!

        NOTE -- No change to curr_inforce_df when the 'new_end_dt' is in the same mth as curr_dt, and both days are >= 2nd BD.  If they are either diff months, or one is before and one is after the 2nd BD, the data must be updated!

        TODO -- Make sure the 'new_end_dt' is a valid business day!
        """

        new_attrib_start_dt = prev_bd(attrib_end_dt_new)
        new_attrib_end_dt = attrib_end_dt_new

        print(f'Attempting to update prev dt from {self.attrib_start_dt} to {new_attrib_start_dt}, and curr dt from {self.attrib_end_dt} to {attrib_end_dt_new}')

        if new_attrib_end_dt == self.attrib_end_dt:
            print(f'You entered the same end date of {self.attrib_end_dt}!  No Further Changes will be made.')
            return
                

        self.reset_all_dfs()
        self.attrib_start_dt = new_attrib_start_dt
        self.attrib_end_dt = new_attrib_end_dt
        self.first_bd = get_first_bd(self.attrib_end_dt, self.assum_dfs['HedgeDates'])
        self.second_bd = get_second_bd(self.attrib_end_dt, self.assum_dfs['HedgeDates'])
        self.hedge_dt = date((self.first_bd).year, self.first_bd.month, 1)


    def reset_all_dfs(self):
        # Initialize Empty DataFrames        
        # self.attrib_summary_df = None        
        self.position_attrib_df = None

    def get_position_attrib_df(self):

        """See comments below for why this section no longer needed
        # self.attrib_plcy_lvl_df = self.create_attrib_plcy_lvl_df()
        # self.attrib_summary_df = self.create_attrib_summary_df()
        """
        # Technically <based on design> if we call the below function, the creation of all it's dependencies <the tables/dfs created 1st that it depends on> will cascade!
        
        if self.raw_holdings_df is None:
            self.load_raw_holdings_df()

        self.position_attrib_df = self.create_attrib_df()

        print(f'Fininshed Updating Orion Asset Position Attribution Data for {self.attrib_start_dt} to {self.attrib_end_dt}')
        
        return self.position_attrib_df

                
    def load_raw_holdings_df(self):

        if self.raw_holdings_df is not None:
            return self.raw_holdings_df

        date_flds = ['HedgeDt','Seg_StartDt','TradeDt','SettleDt','ExpiryDt']
        
        raw_holdings_df = read_excel_df_with_dates(self.__asset_holdings_file, self.__holdings_file_sheet_name, date_flds)
            
        raw_holdings_df['Ntnl'] = raw_holdings_df.loc[:, 'Notional'] * raw_holdings_df.loc[:, 'OrionPct']
        raw_holdings_df['Contracts_PreOrionPct'] = raw_holdings_df.loc[:, 'Contracts']
        raw_holdings_df['Contracts'] = raw_holdings_df.loc[:,'Contracts_PreOrionPct'] * raw_holdings_df.loc[:, 'OrionPct']
        raw_holdings_df['Rate'] = raw_holdings_df.loc[:,'Cap/Rate']    
        raw_holdings_df['TradePrice_Entry'] = raw_holdings_df.loc[:, 'TradePrice']
                                                                
        self.raw_holdings_df = raw_holdings_df
        return self.raw_holdings_df

    
    def create_attrib_df(self):

        # Only create it if it is None
        if self.position_attrib_df is not None:
            return self.position_attrib_df

        # print('Creating Position File')

        # Start w/ a fresh copy of the raw data
        position_attrib_df = self.raw_holdings_df.copy(deep=True)

        # Remove records where Segment has already matured
        position_attrib_df = position_attrib_df[position_attrib_df['ExpiryDt'] > self.attrib_start_dt]
        position_attrib_df = position_attrib_df[position_attrib_df['TradeDt'] <= self.attrib_end_dt]

        # np.where(df['col1'] > 3, 'High', 'Low')
        position_attrib_df['Ntnl_BoP'] = np.where(position_attrib_df['TradeDt']==self.attrib_end_dt, 0.0, position_attrib_df['Ntnl'])
        position_attrib_df['Ntnl_Added'] = np.where(position_attrib_df['TradeDt']==self.attrib_end_dt, position_attrib_df['Ntnl'],  0.0)
        position_attrib_df['Ntnl_Matured'] = np.where(position_attrib_df['ExpiryDt']==self.attrib_end_dt, -position_attrib_df['Ntnl'],  0.0)
        position_attrib_df['Ntnl_EoP'] = np.where(position_attrib_df['ExpiryDt']==self.attrib_end_dt, 0.0, position_attrib_df['Ntnl'])

        position_attrib_df['Attrib_StartDt'] = self.attrib_start_dt
        position_attrib_df['Attrib_EndDt'] = self.attrib_end_dt



        """ TODO: Order the resulting columns """
        # ordered_cols = ['Attrib_Type', 'Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Bbg_Idx','Fund_Name','Opt_Type', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio', 'PolicyCount', 'Base_Liab_Ntnl']
        ordered_cols = ['Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt', 'Seg_StartDt', 'TradeDt', 'ExpiryDt', 'CompID', 'Bbg_Idx','Fund_Name','Opt_Type', 'TradePrice_Entry', 'IdxLvl_StartDt', 'Strike_Low', 'Strike_High', 'Rate', 'Ntnl_BoP', 'Ntnl_Added', 'Ntnl_Matured', 'Ntnl_EoP']
        position_attrib_df = position_attrib_df[ordered_cols]

        # print(f'Finished Creation of Orion Asset Position DataFrame for Attribution Period {self.attrib_start_dt} to {self.attrib_end_dt}')

        """ Store & Return Result """
        self.position_attrib_df = position_attrib_df
        return self.position_attrib_df
    

    def get_position_df(self, val_dt: date):
        """
        """
        # First Make Sure We Have the Data Loaded
        if self.raw_holdings_df is None:
            self.load_raw_holdings_df()

        # Next, Filter the Data to the Desired Date
        # Start w/ a fresh copy of the raw data
        position_df = self.raw_holdings_df.copy(deep=True)

        # Remove records where Segment has already matured
        position_df = position_df[position_df['ExpiryDt'] >= val_dt]
        position_df = position_df[position_df['TradeDt'] <= val_dt]

        # np.where(df['col1'] > 3, 'High', 'Low')
        position_df['Ntnl_BoP'] = np.where(position_df['TradeDt']==self.attrib_end_dt, 0.0, position_df['Ntnl'])
        position_df['Ntnl_EoP'] = np.where(position_df['ExpiryDt']==self.attrib_end_dt, 0.0, position_df['Ntnl'])

        position_df['ValDt'] = val_dt
        



        """ TODO: Order the resulting columns """
        # ordered_cols = ['Attrib_Type', 'Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Bbg_Idx','Fund_Name','Opt_Type', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio', 'PolicyCount', 'Base_Liab_Ntnl']
        ordered_cols = ['ValDt', 'HedgeDt', 'Seg_StartDt', 'TradeDt', 'ExpiryDt', 'CompID', 'Bbg_Idx','Fund_Name','Opt_Type', 'IdxLvl_StartDt', 'Strike_Low', 'Strike_High', 'Rate', 'Ntnl_BoP', 'Ntnl_EoP']
        position_df = position_df[ordered_cols]
    
            
    def create_output_fldr(self):
        
        # NOTE: Would like to move the creation of output folder to file_utils.py!

        output_path = path.join(self.__base_output_path, self.attrib_end_dt.strftime('%Y%m'), 'Attrib')

        if not path.exists(output_path):
            os.makedirs(output_path)

        return output_path
    
    def save_all_results(self):
        pass
        """ Nothing to save for Orion Assets (other than PnL results after running through the model)
        self.output_path = self.create_output_fldr()

                        
        xl_file_name = 'Orion_IUL_Notional_Attribution_Details_' + '' + '.xlsx'
        xl_path = path.join(self.output_path, xl_file_name)

        if os.path.isfile(xl_path):
            print('Ntnl Attrib file already exists!  If you want to creete a new file, delete the old file first!')
            return

        # Define a dictionary of sheets and dataframes to output to Summary Workbook
        sheet_to_df_dict = {
            'PlcyCnt' : self.plcy_cnt_pivot,
            'Base_Ntnl' : self.base_ntnl_pivot,
            'Adj_Ntnl' : self.adj_ntnl_pivot,

        }
        
        summarize_to_xl(xl_path, sheet_to_df_dict)
        """


    @property
    def attrib_detail_sheet_name(self):
        return self.position_type + '_DetailedAttrib'
    
    @property
    def attrib_date_flds(self):
        return ['Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt','Seg_StartDt','TradeDt','ExpiryDt']
    
    @property
    def position_type(self):
        return 'OrionAsset'

def run_debug_test(attr_end_dt: date):

    # Create attr obj and run everything
    attr = OrionAsset(attrib_end_dt=attr_end_dt)
    attr.get_position_attrib_df()

    # Create test position
    position_df_dict = attr.position_attrib_df.to_dict(orient='records')
    test_position = position_df_dict[0]

    # extract some paramaters and load mkt data
    Attrib_StartDt = test_position['Attrib_StartDt']
    Attrib_EndDt = test_position['Attrib_EndDt']
    Bbg_Idx = test_position['Bbg_Idx']
    tickers = attr.position_attrib_df['Bbg_Idx'].unique()
    mds = MktData(tickers)
    mds.load_implied_vol(Attrib_StartDt)
    mds.load_implied_vol(Attrib_EndDt)

    # Create a Position
    iul_pos = Position(**test_position)

    # try and conduct the calculation
    attrib_results_detailed = iul_pos.calc_attrib(mds, Attrib_StartDt, Attrib_EndDt, debug_mode=False)


# @timing
def run_asset_attr_test(attr_end_dt: date):
    
    # Create attr obj and run everything
    attr = OrionAsset(attrib_end_dt=attr_end_dt)
    attr.get_position_attrib_df()

    tickers = attr.position_attrib_df['Bbg_Idx'].unique()
    mds = MktData(tickers)

    for dt in [attr.attrib_start_dt, attr.attrib_end_dt]:
        mds.load_implied_vol(dt)

    position_attrib_df = attr.position_attrib_df.copy(deep=True)

    # for row in attr.position_df.to_dict(orient='records'):
    for df_idx, row in position_attrib_df.to_dict(orient='index').items():

        position = Position(**row)

        # for testing purposes
        # maturity_dt = row['ExpiryDt']
        # debug_dt = date(2025, 1, 31)
        # if maturity_dt == debug_dt:
        #     print('Starting Debug!')

        position_attr_results = position.calc_attrib(mds, attr.attrib_start_dt, attr.attrib_end_dt)

        for k, v in position_attr_results.items():
            position_attrib_df.at[df_idx, k] = v

    
    output_path = attr.create_output_fldr()

    xl_file_name = 'Orion_Asset_Attrib_Results_TEST' + '.xlsx'
    xl_path = path.join(output_path, xl_file_name)

    # TODO:  Include Summary of Daily Mkt Changes?

    # Define a dictionary of sheets and dataframes to output to Summary Workbook
    sheet_to_df_dict = {
        'AssetAttrib_Detailed' : position_attrib_df,
        # 'Base_Ntnl' : self.base_ntnl_pivot,
        # 'Adj_Ntnl' : self.adj_ntnl_pivot,
        # 'Summary_New_Cohort' : self.liab_summary_curr_mth_df,
        # 'Trading_Summary' : self.trading_summary_df_xl
    }

    summarize_to_xl(xl_path, sheet_to_df_dict)

@timing
def run_start_to_end_test(attr_start_dt: date, attr_end_dt: date):
    
    # Create attr obj and run everything
    tmp_start_dt = attr_start_dt
    tmp_end_dt = next_bd(tmp_start_dt)

    attr = OrionAsset(attrib_end_dt=tmp_end_dt)
    attr.get_position_attrib_df()

    tickers = attr.position_attrib_df['Bbg_Idx'].unique()
    mds = MktData(tickers)

    all_positions = None

    while tmp_end_dt <= attr_end_dt:
            
        # Only update if we aren't in the 1st iteration
        if tmp_end_dt != next_bd(tmp_start_dt):
            attr.update_attrib_end_dt(tmp_end_dt)
            attr.get_position_attrib_df()

        # Load Market Data for the new dates
        for dt in [attr.attrib_start_dt, attr.attrib_end_dt]:
            mds.load_implied_vol(dt)

        # Copy out results of new position
        position_df = attr.position_attrib_df.copy(deep=True)

        # for row in attr.position_df.to_dict(orient='records'):
        for df_idx, row in position_df.to_dict(orient='index').items():

            position = Position(**row)

            # for testing purposes
            # maturity_dt = row['ExpiryDt']
            # debug_dt = date(2025, 1, 31)
            # if maturity_dt == debug_dt:
            #     print('Starting Debug!')

            position_attr_results = position.calc_attrib(mds, attr.attrib_start_dt, attr.attrib_end_dt)

            # Output Results to Dataframe
            for k, v in position_attr_results.items():
                position_df.at[df_idx, k] = v

        # Update the 'all_positions' dataframe
        if all_positions is not None:
            all_positions = pd.concat([all_positions, position_df], ignore_index=True).reset_index(drop=True)
        else:
            all_positions = position_df.copy(deep=True)

        # Move to the next business date
        tmp_end_dt = next_bd(tmp_end_dt)

    # Create output folder if not yet created
    output_path = attr.create_output_fldr()

    xl_file_name = 'Orion_Asset_Attrib_Results_TEST' + '.xlsx'
    xl_path = path.join(output_path, xl_file_name)

    # TODO:  Include Summary of Daily Mkt Changes?

    # Define a dictionary of sheets and dataframes to output to Summary Workbook
    sheet_to_df_dict = {
        'AssetAttrib_Detailed' : all_positions,
        # 'Base_Ntnl' : self.base_ntnl_pivot,
        # 'Adj_Ntnl' : self.adj_ntnl_pivot,
        # 'Summary_New_Cohort' : self.liab_summary_curr_mth_df,
        # 'Trading_Summary' : self.trading_summary_df_xl
    }

    summarize_to_xl(xl_path, sheet_to_df_dict)

            
if __name__ == "__main__":

    # attr_test_date_1 = date(2025, 1, 2)
    # attr_test_1 = InforceAttrib(curr_dt=attr_test_date_1)
    # attr_test_1_df = attr_test_1.create_attrib_plcy_lvl_df()

    # run_debug_test(date(2025, 1, 3))
    # run_asset_attr_test(date(2025, 1, 3))
    # run_liab_attr_test(date(2024, 12, 31))

    # run_start_to_end_test(date(2025, 1, 2), date(2025, 2, 5))
    run_start_to_end_test(date(2024, 12, 31), date(2025, 2, 5))

    """ Run a List of dates
    date_list = [date(2025, 1, 3)]
    
    for dt in date_list:
        if attr is None:
            attr = InforceAttrib(curr_dt=dt)
        else:
            attr.update_attrib_end_dt(dt)
        attr.run_all()
        # attr.save_all_results()
    """




