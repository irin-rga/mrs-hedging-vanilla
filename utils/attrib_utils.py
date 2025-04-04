import pandas as pd

def create_attrib_df(prev_df, curr_df, join_cols, chg_cols):
    """
    Creates a dataframe from removed items, new items and change in existing items
    """
    attrib_df = pd.concat([get_removed_items(prev_df, curr_df, join_cols, chg_cols), 
                           get_new_items(prev_df, curr_df, join_cols, chg_cols), 
                           get_changed_items(prev_df, curr_df, join_cols, chg_cols)]
                           ).reset_index(drop=True)
    
    return attrib_df

def get_changed_items(prev_df, curr_df, join_cols, chg_cols):
    """
    Returns a dataframe of changed items that exist both on the orig and true-up files
    """
    chg_cols_prev = { col: col + '_Prev' for col in chg_cols}
    chg_cols_curr = { col: col + '_Curr' for col in chg_cols}

    prev_df = prev_df.rename(columns=chg_cols_prev, errors="raise")
    curr_df = curr_df.rename(columns=chg_cols_curr, errors="raise") # .drop(columns=['Record_Type', 'Co_Code', 'Val_Date', 'Best_Val_Ind'])

    chg_df = pd.merge(prev_df, curr_df, how='inner', on=join_cols)
    
    chg_df['Attrib_Type'] = 'Chg'

    final_cols = ['Attrib_Type'] + join_cols

    for col in chg_cols:
        chg_df[col + '_Chg'] = chg_df[col + '_Curr'] - chg_df[col + '_Prev']
        final_cols = final_cols + [col + '_Prev', col + '_Chg', col + '_Curr']

    chg_df = chg_df[final_cols]
    
    return chg_df

def get_new_items(prev_df, curr_df, join_cols, chg_cols):
    """
    Returns a dataframe containing only records that are new
    """    
    chg_cols_prev = { col: col + '_Prev' for col in chg_cols}
    chg_cols_curr = { col: col + '_Curr' for col in chg_cols}

    prev_df = prev_df.rename(columns=chg_cols_prev, errors="raise")
    curr_df = curr_df.rename(columns=chg_cols_curr, errors="raise") # .drop(columns=['Record_Type', 'Co_Code', 'Val_Date', 'Best_Val_Ind'])

    new_df = pd.merge(prev_df, curr_df, how='outer', on=join_cols, indicator=True)
    new_df = pd.DataFrame(new_df[new_df['_merge'] == 'right_only'])

    new_df['Attrib_Type'] = 'Added'

    final_cols = ['Attrib_Type'] + join_cols

    for col in chg_cols:
        new_df[col + '_Prev'] = 0.0
        new_df[col + '_Chg'] = new_df[col + '_Curr']
        final_cols = final_cols + [col + '_Prev', col + '_Chg', col + '_Curr']
    
    new_df = new_df[final_cols]

    return new_df

def get_removed_items(orig_df, true_df, join_cols, chg_cols):
    """
    Returns a dataframe containing only records that have been removed
    """
    chg_cols_prev = { col: col + '_Prev' for col in chg_cols}
    chg_cols_curr = { col: col + '_Curr' for col in chg_cols}

    prev_df = orig_df.rename(columns=chg_cols_prev, errors="raise")
    curr_df = true_df.rename(columns=chg_cols_curr, errors="raise") # .drop(columns=['Record_Type', 'Co_Code', 'Val_Date', 'Best_Val_Ind'])
        
    decrements_df = pd.merge(prev_df, curr_df, how='outer', on=join_cols, indicator=True)
    
    decrements_df = pd.DataFrame(decrements_df[decrements_df['_merge'] == 'left_only'])
    
    decrements_df['Attrib_Type'] = 'Decr'

    final_cols = ['Attrib_Type'] + join_cols

    for col in chg_cols:
        decrements_df[col + '_Chg'] = -decrements_df[col + '_Prev']
        decrements_df[col + '_Curr'] = 0.0        
        final_cols = final_cols + [col + '_Prev', col + '_Chg', col + '_Curr']

    decrements_df = decrements_df[final_cols]

    return decrements_df

def adjust_inf_df(df: pd.DataFrame, hdg_dts_df: pd.DataFrame):
    # Ensure HedgeDates have been converted to dates!
    df['HedgeDt'] = pd.to_datetime(df['HedgeDt']).dt.date
            
    df_cols = list(df.columns)[1:]
    df_cols_adj = ['HedgeDt', 'Seg_StartDt', 'Seg_EndDt'] + df_cols

    # Add in ExpiryDt to prev and curr df's
    df = pd.merge(df, hdg_dts_df, on='HedgeDt')

    df['Seg_EndDt'] = pd.to_datetime(df['Seg_EndDt']).dt.date
    df['Seg_StartDt'] = pd.to_datetime(df['Seg_StartDt']).dt.date
    
    # Reduce to just original cols plus ExpiryDt
    df = df[df_cols_adj]

    # Eliminate any cases where Base_Liab_Ntnl is zero
    df = df[df['Base_Liab_Ntnl']!=0]
    
    # Add 'HdgFctr' to prev and curr df's
    df['HdgFctr'] = df['Target_Liab_Ntnl'] / (df['Base_Liab_Ntnl'] * df['Ntnl_Mult'])
    df['HdgFctr'] = df['HdgFctr'].round(4)
    df['Cap'] = df['Cap'].round(4)

    # Return the modified dataframe
    return df
