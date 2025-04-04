from xlsxwriter.utility import xl_rowcol_to_cell
from .date_utils import convert_to_date
from typing import List
# from pandas import DataFrame as df
import pandas as pd
from os import path
import time
import os


def summarize_to_xl(xl_path, sheet_to_df_dict):
        
        # TODO:  Check to see if parent directory of xl_path exists, and if not, create it!
        
        # eng_kwargs = {'options': {'strings_to_numbers': True}}
        # with pd.ExcelWriter(test_xl_file, engine='xlsxwriter', engine_kwargs=eng_kwargs) as writer:
        
        print(f'Starting to save results to {xl_path}')
        
        with pd.ExcelWriter(xl_path, engine='xlsxwriter') as writer:
                    
            # Get Workbook
            xl_wb =  writer.book
            
            # Add a header format and formats for other common data types.
            header_format = xl_wb.add_format(
            {
                "border": 0,
                "bold": True,
                "text_wrap": False,                
                "align": "center",
                "bg_color": "#D9D9D9",
                "font_color": "#0070C0",                
            }
            )
            date_fmt = xl_wb.add_format({"num_format": "yyyy/mm/dd", "align": "center"})
            non_numeric_fmt = xl_wb.add_format({"align": "center"})
            pct_fmt = xl_wb.add_format({"num_format": "0.00%", "align": "center"})
            num_fmt = xl_wb.add_format({"num_format": "#,##0", "align": "center"})

            dt_flds = ['HedgeDate', 'HedgeDt', 'AsOfDt', 'AsOf_Date',  'ExpiryDt', 'InforceDt', 'Inforce_AsOfDt', 'IssueDate', 'FirstBD', 'Seg_StartDt', 'Seg_EndDt', 'Entry_Date', 'IdxLvl_EndDt_DateUsed', 'Attrib_StartDt', 'Attrib_EndDt']
            pct_flds = ['Part', 'Cap', 'Cap/Rate', 'Rate', 'Floor',	'Spec_Rate', 'Spread', 'Asset_Charge', 'Multiplier', 'Strike', 'Budget', 'HdgFctr', 'HedgeRatio', 'HedgeAssetPct', 'Coins', 'Ntnl_Mult', 'Idx_Rtn', 'Payoff_PctRtn', 'Idx_Credit_Rt', 'Segment_Idx_Credit_Rt']
            numeric_flds = ['Idx_Credit', 'Idx_Credit_Amt', 'Idx_Credit_Net_Coins', 'PolicyNum', 'PolicyCnt', 'PolicyCount', 'Base_Liab_Ntnl', 'Adj_Liab_Ntnl', 'Pre_HdgRatio_Ntnl', 'Notional', 'Ntnl', 'Target_Liab_Ntnl', 'IdxLvl_StartDt', 
                            'IdxLvl_EndDt', 'Strike_Low', 'Strike_High', 'Implied_Ntnl', 'BoP','Added','Chg','Decr','Matured','EoP','MV_BoP','MV_Chg_New', 'MV_Chg_Ntnl_Added','MV_Chg_Ntnl_Chg','MV_Chg_Ntnl_Decr','MV_Chg_Ntnl_Matured','MV_Chg_PayOff',
                            'MV_Chg_Spot','MV_Chg_RFR','MV_Chg_Dvd','MV_Chg_Vol','MV_Chg_Time','MV_EoP', 'Ntnl_BoP','Ntnl_Added','Ntnl_Chg','Ntnl_Decr','Ntnl_Matured','Ntnl_EoP', 'Ttl_Chgs', 'MV_EoP-MV_BoP', 'Check']

            # Helper Function to Apply the correct format to data based on data type
            def get_fmt_type(col_name):
                if col_name in pct_flds:
                    return pct_fmt
                elif col_name in numeric_flds:
                    return num_fmt
                elif col_name in dt_flds:
                    return date_fmt
                else:
                    return non_numeric_fmt

            # Converts the data range that a dataframe will take up on the spreadsheet to range notation (aka outpouts 'A1:E83' for example, used in applying auto-filters)
            def get_xl_rng_for_df(df: pd.DataFrame):
                rows, columns = df.shape
                return xl_rowcol_to_cell(0,0) + ':' + xl_rowcol_to_cell(rows, columns-1)
                  
            # Helper function that takes a dataframe and sheet name, then writes the dataframe data to the sheet w/ desired formatting
            def write_df_to_sheet_w_fmt(df: pd.DataFrame, sheet_name):

                # Create the sheet and write to it
                df.to_excel(writer, index=False, header=True, sheet_name=sheet_name, startrow=0 , startcol=0)
                # Get Worksheet Object
                xl_ws = writer.sheets[sheet_name]
                # Turn of Gridlines
                xl_ws.hide_gridlines(2)
                # Write the column data with the defined format.
                for col_num, col_name in enumerate(df.columns.values):

                    col_width = 40 if col_name in ['Fund_Name', 'Idx_Credit_Net_Coins'] else 15
                    xl_ws.set_column(col_num, col_num, col_width, get_fmt_type(col_name))            
                    if col_name in dt_flds:
                        for row, val in enumerate(df[col_name]):
                            xl_ws.write_datetime(row+1, col_num, val, date_fmt)
                # Format headers
                for col_num, value in enumerate(df.columns.values):            
                    xl_ws.write(0, col_num, value, header_format)                    
                # Freezing top row 
                xl_ws.freeze_panes(1, 0)
                # Apply auto-filter
                xl_ws.autofilter(get_xl_rng_for_df(df))
                
            for sheet, df in sheet_to_df_dict.items():
                # print(f'Saving results to {sheet}')
                write_df_to_sheet_w_fmt(df, sheet)

def save_results(output_fldr, df_filename: str, df: pd.DataFrame, keep_idx: bool=False, idx_lbl: str=None):
        
    if not path.exists(output_fldr):
        os.makedirs(output_fldr)
        
    print(f'Saving {df_filename} to: {output_fldr}')        
    
    full_path = path.join(output_fldr, df_filename)            
    
    save_start = time.time()

    if df_filename.endswith('.csv'):
        df.to_csv(full_path, index=keep_idx, index_label=idx_lbl)
    elif df_filename.endswith('.xlsx'):
        df.to_excel(full_path, index=keep_idx, index_label=idx_lbl)

    save_end = time.time()
    save_time = save_end - save_start
    print(f"Time spend saving {df_filename}: {int(save_time // 60)} mins {int(save_time % 60)} secs")


def read_excel_df_with_dates(xl_file_path: str, sheet_name: str, date_flds: List[str]):

    # Create a date converter for pandas to read date cols in excel as dates
    # date_flds = ['HedgeDt']
    converters = {col: convert_to_date for col in date_flds}

    xl_df = pd.read_excel(xl_file_path, sheet_name=sheet_name, converters=converters)

    return xl_df