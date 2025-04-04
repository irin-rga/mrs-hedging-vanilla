from typing import Optional, Union, List
from datetime import datetime, date
from pathlib import Path
import pandas as pd
from os import path
import os
from utils.date_utils import get_second_bd, prev_bd, get_full_seriatim_inforce_file, get_first_bd, next_bd
from utils.file_utils import summarize_to_xl, save_results
from HedgeModel.MktData.mkt_data import MktData
from HedgeModel.positions import Position
from utils.attrib_utils import create_attrib_df, get_changed_items, get_new_items, get_removed_items, adjust_inf_df
from utils.assumption_loader_utils import load_static_assumptions
from utils.decoration_utils import timing, timer



#TODO:  Move this to attribution helper functions module!!!
# Create pivot function
def create_attrib_pivot(df: pd.DataFrame, pivot_idx_cols: List[str], piv_col: str, val_col: str, all_pivot_col_categories_ordered: List[str], aggfunc='sum', fill_value=0):

    piv = df.copy(deep=True)
    piv = piv.pivot_table(index=pivot_idx_cols, columns=piv_col, values=val_col, aggfunc=aggfunc, fill_value=fill_value).reset_index()

    # Make sure all the columns are present
    for col in all_pivot_col_categories_ordered:
        if col not in piv.columns:
            piv[col] = 0.0

    # Now order the pivots columns
    all_piv_cols_ordered = pivot_idx_cols + all_pivot_col_categories_ordered
    piv = piv[all_piv_cols_ordered]

    # return the pivot
    return piv


class OrionInforce:

    # __static_assum_fldr = os.path.join(os.getcwd(), 'Static_Assumptions')
    # __static_assum_fldr = os.path.join(Path().resolve().parents[0], 'Static_Assumptions')
    # __static_assum_fldr = os.path.join(Path().resolve().parents[0], 'Static_Assumptions')

    # __static_assum_fldr = r'\\rgare.net\stlfinmkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\1_Code\Static_Assumptions'
    __static_assum_fldr = r'C:\Users\S0053071\Repos\OrionWinterfell_Hedging\dev\Static_Assumptions'

    

    # Assum files as list (all csv files w/out the .csv so that names can be used as keys to dataframes containing their data!)
    __assum_files = ['HedgeDates', 'HdgFctrLU', 'CoPlanInd_to_Prod', 'Indicator_to_FundName', 'ProductDetailsByHedgeDate', 'Orion_IUL_Policies']

    # Add hedge_file_path Default location when available
    # NOTE:  This is the ULTIMATE LOCATION (PROD)
    # __hedge_file_fldr = r"\\rgare.net\STLCommon\ADMIN\NonTrad\CIMBA\CIMBA Prod\Anico\Imported Client Files"

    # These are TEMPORARY LOCATIONS  NOTE: SWITCH TO DEFAULT ABOVE WHEN CIMBA PROD HAS BEEN SETUP!!!
    # __hedge_file_fldr = "\\\\rgare.net\\stlcommon\\ADMIN\\NonTrad\\CIMBA\\CIMBA Prod\\Anico\\Received Client Files"
    __hedge_file_fldr = r"\\rgare.net\STLCommon\ADMIN\NonTrad\CIMBA\CIMBA Stage\Anico\Imported Client Files"

    # __base_output_path = '\\\\rgare.net\\stlfinmkts\\MarketRiskMgmt\\Pricing Requests\\2024-Orion - IUL Hedging\\RGA_Process\\2_Results'
    __base_output_path = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Results'

    __processed_inforce_fldr = r'C:\Users\S0053071\Repos\Orion_Process_Backup\HdgRpts_Results'

    # MM_YYYY_HEDGE_ORIG eg) 11_2024_HEDGE_ORIG.txt
    # MM_YYYY _HEDGE_TRUE_UP eg) 11_2024_HEDGE_TRUE_UP.txt

    
    def __init__(self, attrib_start_dt : Optional[Union[date, None]] = None, attrib_end_dt: Optional[Union[date, None]] = None):
        """
        is_true_up -- Use False for Initial Hedge File, Use True for True-up Hedge File.  
        """
        self.hedgefile_df = self.inforce_summary_df = self.liab_summary_df = self.trading_summary_df = self.trading_summary_full_df = self.trading_summary_df_xl = self.updated_productdetails_df  = None
        self.inforce_summary_curr_mth_df = self.liab_summary_curr_mth_df = self.idx_credit_df = None
        self.inforce_dt = self.first_bd = self.hedge_dt = self.second_bd =  None
        self.assum_dfs = self.hdgfctr_dict = self.output_path = None                
        self.is_true_up = None

        # Initialize Dates
        self.attrib_end_dt = self.attrib_start_dt = self.first_bd = None

        # Initialize Empty DataFrames
        self.prev_inforce_df = self.curr_inforce_df = None
        self.attrib_plcy_lvl_df = None
        self.attrib_summary_df = None
        self.plcy_cnt_pivot = None
        self.base_ntnl_pivot = None
        self.adj_ntnl_pivot = None
        self.position_attrib_df = None
                
        # Setup By Reading Basic Assumptions
        self.setup(attrib_start_dt, attrib_end_dt)

    def setup(self, attrib_start_dt, attrib_end_dt):
        """
        Make sure to include validation of
        """
        # Load data using the new utility function
        self.assum_dfs = load_static_assumptions(assum_files=self.__assum_files)
        
        self.attrib_end_dt = self.resolve_attrib_end_dt(attrib_end_dt)
        self.attrib_start_dt = self.resolve_attrib_start_dt(attrib_start_dt)

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
        
    
    def update_attrib_end_dt(self, new_attrib_end_dt: date):
        """
        Updates the 'curr_dt' which represents the end date of the daily attribution.  This will allow attribution data to be updated w/out reloading everything
        when unloading would be unnecessary!

        NOTE -- No change to curr_inforce_df when the 'new_end_dt' is in the same mth as curr_dt, and both days are >= 2nd BD.  If they are either diff months, or one is before and one is after the 2nd BD, the data must be updated!

        TODO -- Make sure the 'new_end_dt' is a valid business day!
        """

        new_attrib_start_dt = prev_bd(new_attrib_end_dt)
        new_attrib_end_dt = new_attrib_end_dt

        print(f'Attempting to update prev dt from {self.attrib_start_dt} to {new_attrib_start_dt}, and curr dt from {self.attrib_end_dt} to {new_attrib_end_dt}')

        if new_attrib_end_dt == self.attrib_end_dt:
            print(f'You entered the same end date of {self.attrib_end_dt}!  No Further Changes will be made.')
            return
                
        """ Making it easier logic.  Simply reset every time!
        if new_curr_dt.month != self.curr_dt.month:
            print('New month detected.  Resetting all dataframes!')
            self.reset_all_dfs()
        else:
            print('New end date is in the same month.')            
        if new_curr_dt > self.curr_dt:
            
            print('New end date is after current end date')
            
            if self.curr_dt >= self.second_bd:
                
                # Good to go, no changes needed to curr_inforce_df.  Now check Prev BD
                print('Curr Dt and New End Dt are both after 2nd BD!')
                
                if (self.prev_dt < self.second_bd):
                    # Now the new 'previous' inforce should be equal to the current inforce, since the new_prev_dt is after and we are in the same mth!
                    print('Updating the previous inforce to the current inforce since the previous inforce was prior to receiving a new inforce file!')
                    self.prev_inforce_df = self.curr_inforce_df.copy(deep=True)
                # No Need for else, since in the 'else' condition, no changes are needed!                                

            else:
                print('Original end dt was the 1st BD.  Set the new Prev Dt to 1st BD and load the new inforce for the new current dt')
                temp_df = self.curr_inforce_df.copy(deep=True)
                self.reset_all_dfs()
                self.prev_inforce_df = temp_df

        else:
                
            print('New end date is before the current end date')
            
            if new_curr_dt >= self.second_bd:
                # Good to go, no changes needed to curr_inforce_df.  Now check Prev BD
                print('No Need to Change the Ending Inforce, The new end date should use the same file!')
                if (new_prev_dt < self.second_bd):
                    # Now the new 'previous' inforce should be equal to the current inforce, since the new_prev_dt is after and we are in the same mth!
                    print('Updating the previous inforce to the current inforce since the previous inforce was prior to receiving a new inforce file!')
                    self.prev_inforce_df = self.curr_inforce_df
                # No Need for else, since in the 'else' condition, no changes are needed!
            else:
                # Now we need to set the curr_inforce_df to the prev_inforce_df
                temp_df = self.prev_inforce_df.copy(deep=True)
                self.reset_all_dfs()
                self.curr_inforce_df = temp_df

        """

        self.reset_all_dfs()
        self.attrib_start_dt = new_attrib_start_dt
        self.attrib_end_dt = new_attrib_end_dt
        self.first_bd = get_first_bd(self.attrib_end_dt, self.assum_dfs['HedgeDates'])
        self.second_bd = get_second_bd(self.attrib_end_dt, self.assum_dfs['HedgeDates'])
        self.hedge_dt = date((self.first_bd).year, self.first_bd.month, 1)

    def reset_all_dfs(self):
        # Initialize Empty DataFrames
        self.prev_inforce_df = self.curr_inforce_df = None
        self.attrib_plcy_lvl_df = None
        self.attrib_summary_df = None
        self.plcy_cnt_pivot = None
        self.base_ntnl_pivot = None
        self.adj_ntnl_pivot = None
        self.position_attrib_df = None

    def get_position_attrib_df(self):

        """See comments below for why this section no longer needed
        # self.attrib_plcy_lvl_df = self.create_attrib_plcy_lvl_df()
        # self.attrib_summary_df = self.create_attrib_summary_df()
        """
        # Technically <based on design> if we call the below function, the creation of all it's dependencies <the tables/dfs created 1st that it depends on> will cascade!
        self.create_attrib_pivot_summaries()
        self.save_all_results()

        print(f'Fininshed Updating Orion Inforce Position Attribution Data for {self.attrib_start_dt} to {self.attrib_end_dt}')

    
    def get_position_df(self, val_dt: date):
        """
        Gets the Position DataFrame for the given date.  This will be used to get the current position for the given date.
        """
        # Get the position data for the given date
        # TODO -- Need to implement this function to get the position data for the given date
    
        curr_inf_df = self.get_curr_inforce_df(asof_dt=val_dt)


        """ Obtain Fund Related Data to Merge into the Attribution Summary.  This will be needed for Valuation Pricing/Attribution.  """
        ind_to_fund_cols = ['Indicator','Bbg_Idx','Fund_Name','Opt_Type']
        ind_to_fund_df = self.assum_dfs['Indicator_to_FundName']
        ind_to_fund_df = ind_to_fund_df[ind_to_fund_cols]
        """ Conduct the merge"""
        curr_inf_df = curr_inf_df.merge(ind_to_fund_df, on=['Indicator'])

        # Add ValDt
        curr_inf_df['ValDt'] = val_dt


        # for col in all_pivot_col_categories_ordered:
        #     adj_ntnl_pivot[col] = adj_ntnl_pivot[col] * adj_ntnl_pivot['HdgFctr'] * adj_ntnl_pivot['Ntnl_Mult'] / adj_ntnl_pivot['HedgeRatio']


        """ Order the resulting columns """
        ordered_cols = ['ValDt', 'HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Bbg_Idx','Fund_Name','Opt_Type', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio', 'PolicyCount', 'Base_Liab_Ntnl']


        position_df = self.add_required_position_cols(curr_inf_df)

        return position_df

        

        # Replicate wh



    def create_attrib_plcy_lvl_df(self):
        """
        Grabs the Latest Inforce Info for Previous and Current Days and Creates a Policy Level Attribution of the Changes
        """

        # print('Starting Creation of Policy Level Attribution')

        prev_inf_df = self.get_prev_inforce_df()
        curr_inf_df = self.get_curr_inforce_df()
        

        # join_cols = ['HedgeDate', 'ExpiryDt', 'CompID', 'PolicyNum', 'Plan', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio']
        join_cols = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'PolicyNum', 'Plan', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio']
        # rename_cols = ['Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'Target_Liab_Ntnl']
        change_cols = ['Base_Liab_Ntnl']
                
        """ No Longer Used
        # Change the attrib_df AttrType to Maturity in the event that: (attrib_df['ExpiryDt'] == self.curr_dt) and (attrib_df['Attrib_Type'] = 'Chg')
        # changed_to_matured_criteria = (attrib_df['ExpiryDt'] == self.curr_dt) & (attrib_df['Attrib_Type'] == 'Chg')
        # No Need to Do This!
        # changed_to_matured_criteria = (attrib_df['Seg_EndDt'] == self.curr_dt) & (attrib_df['Attrib_Type'] == 'Chg')
        # attrib_df.loc[changed_to_matured_criteria, 'Attrib_Type'] = 'Matured'
        
        # Change the attrib_df AttrType to Maturity in the event that: (attrib_df['ExpiryDt'] == self.curr_dt) and (attrib_df['Attrib_Type'] = 'Chg')
        # added_to_matured_criteria = (attrib_df['ExpiryDt'] == self.curr_dt) & (attrib_df['Attrib_Type'] == 'Added')        
        # This should never happen now that the 2nd BD will always be after the maturity date (so inforce will never change w.r.t. Ntnl for anniversary pols)
        # added_to_matured_criteria = (attrib_df['Seg_EndDt'] == self.curr_dt) & (attrib_df['Attrib_Type'] == 'Added')
        # attrib_df.loc[added_to_matured_criteria, 'Attrib_Type'] = 'Added_but_Maturing'        
        #  REMOVING THIS SECTION SINCE THE SAME SPLIT IS ACCOMPLISHED BY PIVOT/CROSSTAB
        # Further break apart Added into Added - New Cohort vs. Added - Old Cohorts
        # latest_hedge_date = attrib_df['HedgeDt'].max()
        # New Segment
        # added_new_segment_criteria = (attrib_df['HedgeDt'] == latest_hedge_date) & (attrib_df['Attrib_Type'] == 'Added')
        # attrib_df.loc[added_new_segment_criteria, 'Attrib_Type'] = 'Added_New_Segments'
        # Old Segment
        # added_old_segment_criteria = (attrib_df['Attrib_Type'] == 'Added')
        # attrib_df.loc[added_old_segment_criteria, 'Attrib_Type'] = 'Added_Old_Segments'

        # Conduct the attribution based on left/right/inner joins
        attrib_df = create_attrib_df(prev_df, curr_df, join_cols, change_cols)        
        # Create Policy Lvl Attribution DataFrame
        self.attrib_summary_df = self.get_attr_chg_summary(prev_df, curr_df, attrib_df)
        # return result
        return attrib_df
        """

        # print('Finished Creation of Policy Level Attribution')

        # Conduct the attribution based on left/right/inner joins and return the result        
        return create_attrib_df(prev_inf_df, curr_inf_df, join_cols, change_cols)
    

    def create_attrib_summary_df(self):

        # print('Starting Creation of Attribution Summary')

        prev_df = self.get_prev_inforce_df()
        curr_df = self.get_curr_inforce_df()
        attrib_df = self.get_attrib_plcy_lvl_df()
        
        # Create Policy Change Attribution based on rolling from the 'Prev' to the 'Curr' Dates
        # groupby_cols = ['HedgeDate', 'ExpiryDt', 'CompID', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio']
        groupby_cols = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio']
        
        
        # Start with Summarizing the Previous Day by extracting the Initial (BoP) Starting Policy Count and Base Liability Notional Data
        initial_policy_df = prev_df.groupby(groupby_cols, as_index=False).agg({'PolicyNum':'count', 'Base_Liab_Ntnl':'sum'})
        initial_policy_df['Attrib_Type'] = 'BoP'
        # Order & rename results
        ordered_cols = ['Attrib_Type'] + groupby_cols + ['PolicyNum', 'Base_Liab_Ntnl']
        initial_policy_df = initial_policy_df[ordered_cols]
        initial_policy_df = initial_policy_df.rename(columns={'PolicyNum': 'PolicyCount'})
        # Remove records where Segment has already matured
        initial_policy_df = initial_policy_df[initial_policy_df['Seg_EndDt'] > self.attrib_start_dt]

        # Start Crafting Policy Change Attribution on Attribution DataFrame
        attrib_groupby_cols = ['Attrib_Type'] + groupby_cols
        attrib_policy_df = attrib_df.groupby(attrib_groupby_cols, as_index=False).agg({'PolicyNum':'count', 'Base_Liab_Ntnl_Chg': 'sum'})
        attrib_policy_df = attrib_policy_df.rename(columns={'PolicyNum': 'PolicyCount', 'Base_Liab_Ntnl_Chg': 'Base_Liab_Ntnl'})

        
        """ ADDED -- Policies Newly Added to Segments
        # NOTE:  CrossTab/Pivot removes the need for breaking 'Added' into New vs. Old.
        """
        attrib_policy_df_added = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Added'].copy(deep=True)

        """ Chg in Existing
        # Get chg in ntnl for policies that existed in prev and curr inforce, but where the ntnl amt chgd.  
        # NOTE:  This item has no impact on the chg in policy count!
        """        
        attrib_chg_in_ntnl_existing_df = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Chg'].copy(deep=True)        
        attrib_chg_in_ntnl_existing_df['PolicyCount'] = 0
                        
        """Decrements"""
        attrib_policy_df_decr = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Decr'].copy(deep=True)
        attrib_policy_df_decr['PolicyCount'] = -attrib_policy_df_decr.loc[:,'PolicyCount']
        attrib_policy_df_decr['Base_Liab_Ntnl'] = attrib_policy_df_decr.loc[:,'Base_Liab_Ntnl']
                
        """EndOfPeriod (EoP) Amounts
        # TODO:  CHANGE MATURED DATAFRAME TO COME OFF CURR_DF!!!
        # attrib_policy_df_mat = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Matured'].copy(deep=True)
        # attrib_policy_df_mat['PolicyCount'] = -attrib_policy_df_mat.loc[:,'PolicyCount']
        """
        eop_policy_df = curr_df.groupby(groupby_cols, as_index=False).agg({'PolicyNum':'count', 'Base_Liab_Ntnl':'sum'})
        eop_policy_df['Attrib_Type'] = 'EoP'
        eop_policy_df = eop_policy_df[ordered_cols]
        eop_policy_df = eop_policy_df.rename(columns={'PolicyNum': 'PolicyCount'})

        """Carve Maturities out of EoP"""
        attrib_policy_df_mat = eop_policy_df[eop_policy_df['Seg_EndDt'] == self.attrib_end_dt].copy(deep=True)        
        attrib_policy_df_mat['Attrib_Type'] = 'Matured'                
        attrib_policy_df_mat['PolicyCount'] = -attrib_policy_df_mat.loc[:,'PolicyCount']
        attrib_policy_df_mat['Base_Liab_Ntnl'] = -attrib_policy_df_mat.loc[:,'Base_Liab_Ntnl']

        """Change maturing segment ending notional and policy count to 0 in the EoP dataframe (for consistent attribution changes)"""
        eop_policy_df.loc[eop_policy_df['Seg_EndDt'] == self.attrib_end_dt, 'Base_Liab_Ntnl'] = 0.0
        eop_policy_df.loc[eop_policy_df['Seg_EndDt'] == self.attrib_end_dt, 'PolicyCount'] = 0.0
        
        """ No Longer Needed
        # Get chg in ntnl for policies that are maturing in the current df, but in the previous df had a different ntnl amt 
        # NOTE:  This item has no impact on the chg in policy count!  
        This will not be needed since Ntnl won't change on Maturiing policies since policies will guaranteed mature prior to receiving new inforce on Day 2
        attrib_chg_in_ntnl_maturing_df = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Matured'].copy(deep=True)
        attrib_chg_in_ntnl_maturing_df['Attrib_Type'] = 'Chg_in_Ntnl_on_Maturing'
        attrib_chg_in_ntnl_maturing_df['PolicyCount'] = 0
        # attrib_policy_df_added_not_maturing = attrib_policy_df_added[attrib_policy_df_added['ExpiryDt']!=self.curr_dt]
        # Split into Policy Count changes for New Business and Decrements and Maturity
        # New/Added
        # attrib_policy_df_added = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Added'].copy(deep=True)                
        # NOTE:  'Added_but_Maturing' will no longer be created (based on 2nd bd logic).  CrossTab/Pivot removes the need for breaking 'Added' into New vs. Old.
        # attrib_policy_df_added_maturing = attrib_policy_df[attrib_policy_df['Attrib_Type']=='Added_but_Maturing'].copy(deep=True)
        # attrib_policy_df_added = attrib_policy_df[attrib_policy_df['Attrib_Type'].isin(['Added', 'Added_New_Segments', 'Added_Old_Segments'])].copy(deep=True)
        # attrib_policy_df_added = attrib_policy_df[attrib_policy_df['Attrib_Type'].isin(['Added', 'Added_but_Maturing', 'Added_New_Segments', 'Added_Old_Segments'])].copy(deep=True)
        # self.attrib_chg_summary_df = pd.concat([initial_policy_df, attrib_policy_df_added, attrib_policy_df_decr, attrib_policy_df_mat, eop_policy_not_matured_df]).reset_index(drop=True)
        # self.attrib_chg_summary_df = pd.concat([initial_policy_df, attrib_policy_df_added_maturing, attrib_policy_df_added_not_maturing, attrib_policy_df_decr, attrib_policy_df_mat, eop_policy_not_matured_df]).reset_index(drop=True)
        # self.attrib_chg_summary_df = pd.concat([initial_policy_df, attrib_policy_df_added, attrib_chg_in_ntnl_maturing_df, attrib_chg_in_ntnl_existing_df,
        #                                         attrib_policy_df_decr, attrib_policy_df_mat, eop_policy_not_matured_df]).reset_index(drop=True)
        # self.attrib_chg_summary_df = pd.concat([initial_policy_df, attrib_policy_df_added, attrib_chg_in_ntnl_maturing_df, attrib_chg_in_ntnl_existing_df, 
        #                                         attrib_policy_df_decr, attrib_policy_df_mat, eop_policy_not_matured_df]).reset_index(drop=True)
        # Everything at the end of the period that still exists and didn't mature
        # eop_policy_not_matured_df = eop_policy_df[eop_policy_df['ExpiryDt'] > self.curr_dt]                            

        # Don't need to do this anymore.  Just change maturing segment Ending Ntnl to 0        
        # eop_policy_not_matured_df = eop_policy_df[eop_policy_df['Seg_EndDt'] > self.curr_dt]
        """        
        
        """ Combine all of the above pieces into a single dataframe"""
        attrib_summary_df = pd.concat([initial_policy_df, attrib_policy_df_added, attrib_chg_in_ntnl_existing_df, attrib_policy_df_decr, attrib_policy_df_mat, eop_policy_df]).reset_index(drop=True)
        
        """ Obtain Fund Related Data to Merge into the Attribution Summary.  This will be needed for Valuation Pricing/Attribution.  """
        ind_to_fund_cols = ['Indicator','Bbg_Idx','Fund_Name','Opt_Type']
        ind_to_fund_df = self.assum_dfs['Indicator_to_FundName']
        ind_to_fund_df = ind_to_fund_df[ind_to_fund_cols]
        """ Conduct the merge"""
        attrib_summary_df = attrib_summary_df.merge(ind_to_fund_df, on=['Indicator'])

        """ Add the previous and current days to the dataframe """
        attrib_summary_df['Attrib_StartDt'] = self.attrib_start_dt
        attrib_summary_df['Attrib_EndDt'] = self.attrib_end_dt

        """ Order the resulting columns """
        ordered_cols = ['Attrib_Type', 'Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Bbg_Idx','Fund_Name','Opt_Type', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio', 'PolicyCount', 'Base_Liab_Ntnl']
        attrib_summary_df = attrib_summary_df[ordered_cols]

        # print('Finished Creation of Attribution Summary')

        """ Store & Return Result """
        self.attrib_summary_df = attrib_summary_df
        return self.attrib_summary_df
    
            
    def create_attrib_pivot_summaries(self):
        """
        Used to create simple pivots for exporting different views of the attribution to Excel.  Each pivot will be output to a different excel tab.  
        """

        # print('Starting creation of pivot summaries')
        

        
        # Create the desired arguments to pass to the above helper function
        pivot_idx_cols = ['Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt', 'Seg_StartDt', 'Seg_EndDt', 'CompID', 'Indicator', 'Bbg_Idx','Fund_Name','Opt_Type', 'Cap', 'Ntnl_Mult', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio']
        all_pivot_col_categories_ordered = ['BoP','Added','Chg','Decr','Matured','EoP']
        pivot_col = 'Attrib_Type'

        # Get the Attribution Summary DF
        attrib_summary_df = self.get_attrib_summary_df()
        
        # Create pivot for the attribution of policy count changes
        plcy_cnt_pivot = create_attrib_pivot(attrib_summary_df, pivot_idx_cols, pivot_col, 'PolicyCount', all_pivot_col_categories_ordered)

        # Create pivot for the attribution of base notional changes
        base_ntnl_pivot = create_attrib_pivot(attrib_summary_df, pivot_idx_cols, pivot_col, 'Base_Liab_Ntnl', all_pivot_col_categories_ordered)

        # Create pivot for the attribution of adjusted notional changes <aka the liability before hedge ratio> based on the 'Base' ntnl
        adj_ntnl_pivot = base_ntnl_pivot.copy(deep=True)
        for col in all_pivot_col_categories_ordered:
            adj_ntnl_pivot[col] = adj_ntnl_pivot[col] * adj_ntnl_pivot['HdgFctr'] * adj_ntnl_pivot['Ntnl_Mult'] / adj_ntnl_pivot['HedgeRatio']
        
        # Store the Pivot Table Summaries
        self.plcy_cnt_pivot = plcy_cnt_pivot
        self.base_ntnl_pivot = base_ntnl_pivot
        self.adj_ntnl_pivot = adj_ntnl_pivot
        self.position_attrib_df = self.create_position_file(adj_ntnl_pivot)

        # print('Finished creation of pivot summaries and position_df')



    def add_required_position_cols(self, initial_df: pd.DataFrame):
        """
        Add many of the required fields
        """
        tickers = list(initial_df['Bbg_Idx'].unique())
        mktdata = MktData(tickers)

        position_df = initial_df.copy(deep=True)

        position_df['IdxLvl_StartDt'] = position_df.apply(lambda row: mktdata.get_px(row['Seg_StartDt'], row['Bbg_Idx']), axis=1)
        position_df['Strike_Low'] = position_df['IdxLvl_StartDt'] * position_df['Strike']
        position_df['Strike_High'] = position_df['IdxLvl_StartDt'] * (1 + position_df['Cap'])
        position_df['ExpiryDt'] = position_df['Seg_EndDt']
        position_df['Rate'] = position_df['Cap']

        return position_df
    

    def create_position_file(self, adj_ntnl_pivot: pd.DataFrame):


        position_attrib_df = self.add_required_position_cols(adj_ntnl_pivot)
        
        # Pre-pend all Ntnl Change Fields w/ 'Ntnl_' <for setting the NtnlAttrib dataclass>
        all_pivot_col_categories_ordered = ['BoP','Added','Chg','Decr','Matured','EoP']
        col_rename_dict = { col: 'Ntnl_' + col for col in all_pivot_col_categories_ordered}        
        position_attrib_df = position_attrib_df.rename(columns=col_rename_dict, errors="raise")

        return position_attrib_df


        # position_df['Ntnl'] = position_df['BoP']

    def create_output_fldr(self):
        
        # output_path = path.join(self.__base_output_path, self.hedge_date.strftime('%Y%m%d'))
        output_path = path.join(self.__base_output_path, self.attrib_end_dt.strftime('%Y%m'), 'Attrib')

        if not path.exists(output_path):
            os.makedirs(output_path)

        return output_path
    
    def save_all_results(self):
        """
        Put the saving of files all in 1 place so it's easy to comment out any individual save
        """        
        self.output_path = self.create_output_fldr()

        version = "BD1" if (self.attrib_end_dt == self.first_bd) else ("BD2" if (self.attrib_end_dt == self.second_bd) else "BD3")
                
        xl_file_name = 'Orion_IUL_Notional_Attribution_Details_' + version + '.xlsx'
        xl_path = path.join(self.output_path, xl_file_name)

        if os.path.isfile(xl_path):
            print('Ntnl Attrib file already exists!  If you want to creete a new file, delete the old file first!')
            return

        # Define a dictionary of sheets and dataframes to output to Summary Workbook
        sheet_to_df_dict = {
            'PlcyCnt' : self.plcy_cnt_pivot,
            'Base_Ntnl' : self.base_ntnl_pivot,
            'Adj_Ntnl' : self.adj_ntnl_pivot,
            # 'Summary_New_Cohort' : self.liab_summary_curr_mth_df,
            # 'Trading_Summary' : self.trading_summary_df_xl
        }
        
        summarize_to_xl(xl_path, sheet_to_df_dict)

    # region 'Getters'

    def get_prev_inforce_df(self, asof_dt: Optional[Union[date, None]] = None):
        # Grabs & Calculates the Previous DataFrame if it is None, Then/otherwise returns it
        if self.prev_inforce_df is None:
            # print(f'Loading inforce for {self.attrib_start_dt}')
            hdgdts_df = self.assum_dfs['HedgeDates']
            asof_dt = self.attrib_start_dt if asof_dt is None else asof_dt
            df = get_full_seriatim_inforce_file(self.__processed_inforce_fldr, asof_dt, hdgdts_df)
            # Remove data that matured on or prior to prev_dt
            df = adjust_inf_df(df, hdgdts_df)
            df = df[df['Seg_EndDt'] > asof_dt]
            self.prev_inforce_df = df
        
        return self.prev_inforce_df      
    
    def get_curr_inforce_df(self, asof_dt: Optional[Union[date, None]] = None):
        # Grabs & Calculates the Current DataFrame if it is None.  Then/otherwise returns it
        if self.curr_inforce_df is None:
            # print(f'Loading inforce for {self.attrib_end_dt}')
            hdgdts_df = self.assum_dfs['HedgeDates']
            asof_dt = self.attrib_end_dt if asof_dt is None else asof_dt
            df = get_full_seriatim_inforce_file(self.__processed_inforce_fldr, asof_dt, hdgdts_df)
            # Remove Matured Segments except for those that mature ON curr_dt
            df = adjust_inf_df(df, hdgdts_df)
            df = df[df['Seg_EndDt'] >= asof_dt]
            self.curr_inforce_df = df
        
        return self.curr_inforce_df
    
    def get_attrib_plcy_lvl_df(self):
        # Grabs & Calculates the Policy Attribution Dataframe if it is None.  Then/otherwise returns it
        if self.attrib_plcy_lvl_df is None:
            self.attrib_plcy_lvl_df = self.create_attrib_plcy_lvl_df()

        return self.attrib_plcy_lvl_df
    
    def get_attrib_summary_df(self):
        # Grabs & Calculates the Attribution Summary Dataframe if it is None.  Then/otherwise returns it
        if self.attrib_summary_df is None:
            self.attrib_summary_df = self.create_attrib_summary_df()

        return self.attrib_summary_df
    
    @property
    def attrib_detail_sheet_name(self):
        return self.position_type + '_DetailedAttrib'
    

    @property
    def attrib_date_flds(self):
        return ['Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt','Seg_StartDt','Seg_EndDt','ExpiryDt']
    
    @property
    def position_type(self):
        return 'OrionLiab'
        
    # endregion 'Getters'



# region 'Testing'

def run_debug_test(attr_end_dt: date):

    # Create attr obj and run everything
    attr = OrionInforce(attrib_end_dt=attr_end_dt)
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
def run_liab_attr_test(attr_end_dt: date):
    
    # Create attr obj and run everything
    attr = OrionInforce(attrib_end_dt=attr_end_dt)
    attr.get_position_attrib_df()

    tickers = attr.position_attrib_df['Bbg_Idx'].unique()
    mds = MktData(tickers)

    for dt in [attr.attrib_start_dt, attr.attrib_end_dt]:
        mds.load_implied_vol(dt)

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

        for k, v in position_attr_results.items():
            position_df.at[df_idx, k] = v

    
    output_path = attr.create_output_fldr()

    xl_file_name = 'Orion_Attrib_Results' + '.xlsx'
    xl_path = path.join(output_path, xl_file_name)

    # TODO:  Include Summary of Daily Mkt Changes?

    # Define a dictionary of sheets and dataframes to output to Summary Workbook
    sheet_to_df_dict = {
        'LiabilityAttrib_Detailed' : position_df,
        # 'Base_Ntnl' : self.base_ntnl_pivot,
        # 'Adj_Ntnl' : self.adj_ntnl_pivot,
        # 'Summary_New_Cohort' : self.liab_summary_curr_mth_df,
        # 'Trading_Summary' : self.trading_summary_df_xl
    }

    summarize_to_xl(xl_path, sheet_to_df_dict)

# @timing
# @timer
@timing
def run_start_to_end_test(attr_start_dt: date, attr_end_dt: date):
    
    # Create attr obj and run everything
    tmp_dt = attr_start_dt

    attr = OrionInforce(attrib_end_dt=tmp_dt)
    attr.get_position_attrib_df()

    tickers = attr.position_attrib_df['Bbg_Idx'].unique()
    mds = MktData(tickers)

    all_positions = None

    while tmp_dt <= attr_end_dt:
            
        # Only update if we aren't in the 1st iteration
        if tmp_dt != attr_start_dt:
            attr.update_attrib_end_dt(tmp_dt)
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
        tmp_dt = next_bd(tmp_dt)

    # Create output folder if not yet created
    output_path = attr.create_output_fldr()

    xl_file_name = 'Orion_Attrib_Results' + '.xlsx'
    xl_path = path.join(output_path, xl_file_name)

    # TODO:  Include Summary of Daily Mkt Changes?

    # Define a dictionary of sheets and dataframes to output to Summary Workbook
    sheet_to_df_dict = {
        'LiabilityAttrib_Detailed' : all_positions,
        # 'Base_Ntnl' : self.base_ntnl_pivot,
        # 'Adj_Ntnl' : self.adj_ntnl_pivot,
        # 'Summary_New_Cohort' : self.liab_summary_curr_mth_df,
        # 'Trading_Summary' : self.trading_summary_df_xl
    }

    summarize_to_xl(xl_path, sheet_to_df_dict)

# endregion 'Testing'

            
if __name__ == "__main__":

    # attr_test_date_1 = date(2025, 1, 2)
    # attr_test_1 = InforceAttrib(curr_dt=attr_test_date_1)
    # attr_test_1_df = attr_test_1.create_attrib_plcy_lvl_df()

    # run_debug_test(date(2025, 1, 3))
    # run_liab_attr_test(date(2025, 1, 3))
    # run_liab_attr_test(date(2025, 1, 2))
    # run_liab_attr_test(date(2024, 12, 31))

    # run_liab_attr_test(date(2024, 12, 31))
    run_start_to_end_test(date(2025, 1, 2), date(2025, 1, 3))

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




