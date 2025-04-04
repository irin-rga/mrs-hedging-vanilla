from utils.file_utils import summarize_to_xl, read_excel_df_with_dates
from HedgeModel.ntnl_attrib import NtnlAttrib, FullAttrib
from create_orion_liab_position import OrionInforce
from create_orion_asset_position import OrionAsset
from HedgeModel.MktData.mktdatasvc import MktDataSvc
from utils.decoration_utils import timing, timer
from HedgeModel.MktData.mkt_data import MktData
from utils.date_utils import next_bd, prev_bd
from HedgeModel.positions import Position
from datetime import datetime, date
from typing import List
import pandas as pd
from os import path
import argparse
import os


all_summary_cols = NtnlAttrib.field_names() + FullAttrib.field_names()
orion_tickers = ['NDX Index','SPX Index','SPMARC5P Index']
attrib_detail_tab_suffix = '_DetailedAttrib'
sheet_name_suffixes = [attrib_detail_tab_suffix, '_AttribBy_SegFund', '_AttribBy_Fund', '_AttribBy_Date', '_AttribTotals']

# For Calculating the 'Net' Results, AttribTotals can conduct groupby on Attrib_StartDt and Attrib_EndDt
sheet_name_to_groupby_cols = {
    '_AttribBy_SegFund': ['Attrib_StartDt', 'Attrib_EndDt', 'HedgeDt', 'Seg_StartDt', 'ExpiryDt', 'Fund_Name'],
    '_AttribBy_Fund': ['Attrib_StartDt', 'Attrib_EndDt', 'Fund_Name'],
    '_AttribBy_Date': ['Attrib_StartDt', 'Attrib_EndDt'],
    '_AttribTotals': ['Attrib_StartDt', 'Attrib_EndDt'] 
    # '_AttribTotals': []  # No groupby columns for totals
}

def get_attrib_results_file(attrib_instance):
    results_fldr = attrib_instance.create_output_fldr()
    results_file_name = 'Orion_Attrib_Results.xlsx'
    results_file = path.join(results_fldr, results_file_name)
    return results_file

def get_attrib_results(attrib_instance):
    # Returns a dataframe containing the attribution pnl results already run for the same month as the attribution instances attrib_end_dt

    results_file = get_attrib_results_file(attrib_instance)
    results_sheet = attrib_instance.position_type + attrib_detail_tab_suffix
    dt_flds = attrib_instance.attrib_date_flds

    attrib_results_df = None
    
    if os.path.isfile(results_file):
        # File Exists, So Read it and get data!
        attrib_results_df = read_excel_df_with_dates(results_file, results_sheet, dt_flds)
        # If the results file exists, exclude any data from it for the current attrib_start_dt
        attrib_results_df = attrib_results_df[attrib_results_df['Attrib_StartDt'] != attrib_instance.attrib_start_dt]

    return attrib_results_df


def get_attrib_results_agg_summary(detailed_results: pd.DataFrame, sum_cols: List[str], position_type: str):

    # Calculate Results In Aggregate
    attrib_min_dt = detailed_results['Attrib_StartDt'].min()
    attrib_max_dt = detailed_results['Attrib_EndDt'].max()

    ntnl_bop = detailed_results[detailed_results['Attrib_StartDt']==attrib_min_dt]['Ntnl_BoP'].sum()
    mv_bop = detailed_results[detailed_results['Attrib_StartDt']==attrib_min_dt]['MV_BoP'].sum()

    ntnl_eop = detailed_results[detailed_results['Attrib_EndDt']==attrib_max_dt]['Ntnl_EoP'].sum()
    mv_eop = detailed_results[detailed_results['Attrib_EndDt']==attrib_max_dt]['MV_EoP'].sum()

    agg_sum_cols = [col for col in sum_cols if col not in ['Ntnl_BoP', 'MV_BoP', 'Ntnl_EoP', 'MV_EoP']]

    attrib_totals = pd.DataFrame(detailed_results[agg_sum_cols].sum()).transpose()
    attrib_totals['Ntnl_BoP'] = ntnl_bop
    attrib_totals['MV_BoP'] = mv_bop
    attrib_totals['Ntnl_EoP'] = ntnl_eop
    attrib_totals['MV_EoP'] = mv_eop
    attrib_totals['PositionType'] = position_type
    attrib_totals['Attrib_StartDt'] = detailed_results['Attrib_StartDt'].min()
    attrib_totals['Attrib_EndDt'] = detailed_results['Attrib_EndDt'].max()

    ordered_cols = ['PositionType', 'Attrib_StartDt', 'Attrib_EndDt'] + sum_cols
    # ordered_cols = ['Attrib_StartDt', 'Attrib_EndDt'] + sum_cols
    attrib_totals = attrib_totals[ordered_cols]
    # ordered_cols = sum_cols
    # attrib_totals = attrib_totals[ordered_cols]

    # Temporarily Convert to Series
    # temp_series = attrib_totals.iloc[0].squeeze()
    # attrib_totals = temp_series.to_frame(name=position_type)

    return attrib_totals


def get_attrib_summaries(detailed_results: pd.DataFrame, dt_flds: List[str], position_type: str, sheet_to_df_dict: dict):
    """
    Returns a summary of the detailed attribution results
    """
    # Groupby the detailed results by the groupby_cols
    # Sum the sum_cols
    # Rename the sum_cols to the sum_col_lbls
    # Reset the index
    # Return the summarized dataframe
    
    groupby_fund_seg_cols = dt_flds + ['Fund_Name']
    sum_cols = [col for col in all_summary_cols if col in detailed_results.columns]

    # Results By Segment and Fund
    attrib_by_segment_and_fund = detailed_results.groupby(groupby_fund_seg_cols)[sum_cols].sum().reset_index()

    # Results By Fund
    group_by_fund_cols = ['Attrib_StartDt', 'Attrib_EndDt', 'Fund_Name']
    attrib_by_fund = detailed_results.groupby(group_by_fund_cols)[sum_cols].sum().reset_index()

    # Results By Date
    group_by_date_cols = ['Attrib_StartDt', 'Attrib_EndDt']
    attrib_by_date = detailed_results.groupby(group_by_date_cols)[sum_cols].sum().reset_index()

    # Calculate Results In Aggregate
    attrib_totals = get_attrib_results_agg_summary(detailed_results, sum_cols, position_type)
    
    # Put it all together in the form of the dictionary mapping of sheet names to results dataframes
    
    sheet_names = [position_type + suffix for suffix in sheet_name_suffixes]
    results_dfs = [detailed_results, attrib_by_segment_and_fund, attrib_by_fund, attrib_by_date, attrib_totals]

    for sheet_name, df in zip(sheet_names, results_dfs):
        sheet_to_df_dict[sheet_name] = df
        
    return sheet_to_df_dict


def subtract_dataframes(df_a: pd.DataFrame, df_l: pd.DataFrame, groupby_cols: list) -> pd.DataFrame:
    """
    Subtracts the values of df_l from df_a for all columns not in groupby_cols.
    Missing rows in either DataFrame are treated as zeroes.
    
    Args:
        df_a (pd.DataFrame): The first DataFrame (e.g., assets).
        df_l (pd.DataFrame): The second DataFrame (e.g., liabilities).
        groupby_cols (list): The columns used for grouping (not subtracted).
    
    Returns:
        pd.DataFrame: A DataFrame containing the differences.
    """

    # Restrict df_a and df_b to just the groupby columns and the columns to be subtracted
    possible_cols = groupby_cols + all_summary_cols
    
    cols_a = [cols for cols in df_a.columns if cols in possible_cols]
    cols_l = [cols for cols in df_l.columns if cols in possible_cols]
    df_a = df_a[cols_a] 
    df_l = df_l[cols_l]
    
    # df_a = df_a[possible_cols]
    # df_l = df_l[possible_cols]

    # Ensure both DataFrames have the same index and fill missing rows with zeroes
    combined_index = df_a.set_index(groupby_cols).index.union(df_l.set_index(groupby_cols).index)
    df_a = df_a.set_index(groupby_cols).reindex(combined_index, fill_value=0)
    df_l = df_l.set_index(groupby_cols).reindex(combined_index, fill_value=0)

    # Subtract the values of df_l from df_a for all non-groupby columns
    diff_df = df_a.subtract(df_l, fill_value=0)

    # Reset the index to restore the groupby columns
    diff_df = diff_df.reset_index()

    # Reorder the columns to match the original DataFrame
    diff_df = diff_df[possible_cols]

    return diff_df


def get_attrib_summaries_net(sheet_to_df_dict: dict, asset_tab_prefix: str, liab_tab_prefix: str, net_tab_prefix: str = 'OrionNet'):
    """
    Combines asset and liability dataframes in sheet_to_df_dict by adding them together for each sheet_name_suffix.
    """
    # sheet_name_suffixes = ['_DetailedAttrib', '_AttribBy_SegFund', '_AttribBy_Fund', '_AttribBy_Date', '_AttribTotals']
    
    # Loop through the suffixes and combine the asset and liability dataframes except for the first suffix (the detailed attrib)
    for suffix in sheet_name_suffixes[1:]:
        
        asset_sheet_name = asset_tab_prefix + suffix
        liab_sheet_name = liab_tab_prefix + suffix
        net_sheet_name = net_tab_prefix + suffix

        if asset_sheet_name in sheet_to_df_dict and liab_sheet_name in sheet_to_df_dict:
            # Add the asset and liability dataframes together
            net_df = subtract_dataframes(sheet_to_df_dict[asset_sheet_name], sheet_to_df_dict[liab_sheet_name], sheet_name_to_groupby_cols[suffix])

            if suffix == '_AttribTotals':
                orig_cols = net_df.columns
                net_df['PositionType'] = net_tab_prefix
                new_cols_ordered = ['PositionType'] + [col for col in orig_cols if col != 'PositionType']
                net_df = net_df[new_cols_ordered]

            sheet_to_df_dict[net_sheet_name] = net_df

    return sheet_to_df_dict


@timer
def conduct_position_attrib(attrib_instance, mds: MktDataSvc, sheet_to_df_dict):
    """
    Conducts the attribution for either Asset or Liability
    Gets any prior attribution results for the month
    Returns the dataframe of the results month-to-date"
    """
    
    # Run the instance (creates the position_attrib_df) and extract the instance
    attrib_instance.get_position_attrib_df()
    position_attrib_df = attrib_instance.position_attrib_df.copy(deep=True)

    # Print the attribution details
    print(f'Running PnL Attribution for {attrib_instance.__class__.__name__} between {attrib_instance.attrib_start_dt} to {attrib_instance.attrib_end_dt}')

    # Loops through the positions, calculate the attribution results and tack onto the results dataframe
    for df_idx, row in position_attrib_df.to_dict(orient='index').items():

        position = Position(**row)        
        position_attr_results = position.calc_attrib(mds, attrib_instance.attrib_start_dt, attrib_instance.attrib_end_dt)

        # Output Results to Dataframe
        for k, v in position_attr_results.items():
            position_attrib_df.at[df_idx, k] = v

    # Get Existing Attribution Results for this type from File (if Exists)?
    mtd_attr_results_detailed = get_attrib_results(attrib_instance)

    # If there are existing results, append the new results to the existing results
    if mtd_attr_results_detailed is not None:
        mtd_attr_results_detailed = pd.concat([mtd_attr_results_detailed, position_attrib_df], ignore_index=True).reset_index(drop=True)
    else:
        mtd_attr_results_detailed = position_attrib_df.copy(deep=True)

    # Get other summary dataframes        
    sheet_to_df_dict = get_attrib_summaries(mtd_attr_results_detailed, attrib_instance.attrib_date_flds, attrib_instance.position_type, sheet_to_df_dict)

    # Return Results
    return sheet_to_df_dict


@timer
def daily_orion_attrib(mds: MktDataSvc, liab: OrionInforce, asset: OrionAsset):
            
    # Create a dictionary to associate the mapping of results file sheet names to their dataframe content
    sheet_to_df_dict = {}
        
    # Conduct the Attribution for both Asset and Liability
    sheet_to_df_dict = conduct_position_attrib(liab, mds, sheet_to_df_dict)
    sheet_to_df_dict = conduct_position_attrib(asset, mds, sheet_to_df_dict)
    
    # Combine asset and liability dataframes to get net results
    sheet_to_df_dict = get_attrib_summaries_net(sheet_to_df_dict, asset.position_type, liab.position_type, 'OrionNet')
    
    # Doesn't matter which instance we use, get the same results file
    xl_results_file = get_attrib_results_file(liab)

    # Save the results to an Excel File
    summarize_to_xl(xl_results_file, sheet_to_df_dict)


def parse_args():
    parser = argparse.ArgumentParser(description='Run daily Orion attribution.')
    parser.add_argument('--attrib_start_dt', type=str, help='Attribution start date in YYYY-MM-DD format')
    parser.add_argument('--attrib_end_dt', type=str, help='Attribution end date in YYYY-MM-DD format')
    return parser.parse_args()


@timer
def main_run(attrib_start_dt: date, attrib_end_dt: date):
    
    # Create a MktData Instance and load implied vol data for the dates
    mds = MktData(orion_tickers)
    tmp_dt = None
    
    # If no start date, then use the previous business day and start the attribution from the provided or default end date!
    if attrib_start_dt is None:
        attrib_start_dt = prev_bd(attrib_end_dt)
        tmp_dt = attrib_end_dt
    # Otherwise, We assumed the provided start date is the end date of the first attribution date provided!
    else:
        tmp_dt = attrib_start_dt

    # Create Asset & Liability Instances
    liab = OrionInforce(attrib_end_dt=tmp_dt)
    asset = OrionAsset(attrib_end_dt=tmp_dt, assum_dfs=liab.assum_dfs)

    # Loop over the dates and run the daily attribution
    while tmp_dt <= attrib_end_dt:

        # Update Attrib End Dates
        liab.update_attrib_end_dt(tmp_dt)
        asset.update_attrib_end_dt(tmp_dt)

        # Pull Market Data for the new dates
        for dt in [liab.attrib_start_dt, liab.attrib_end_dt]:
            mds.load_implied_vol(dt)

        # Run Daily Attribution
        daily_orion_attrib(mds, liab, asset)
        
        # Move to Next Business Day
        tmp_dt = next_bd(tmp_dt)


if __name__ == "__main__":

    # for col in all_summary_cols:
    #     print(col)

    args = parse_args()
    
    attrib_start_dt = datetime.strptime(args.attrib_start_dt, '%Y-%m-%d').date() if args.attrib_start_dt else None
    attrib_end_dt = datetime.strptime(args.attrib_end_dt, '%Y-%m-%d').date() if args.attrib_end_dt else None

    if attrib_end_dt is None:
        attrib_end_dt = prev_bd(datetime.today())    
    
    # TODO:  Remove this when done testing!!!
    tmp_testing = True    
    if tmp_testing:
        # For Temporary Testing
        attrib_start_dt = date(2025, 3, 28)
        attrib_end_dt = date(2025, 3, 31)

    main_run(attrib_start_dt, attrib_end_dt)

