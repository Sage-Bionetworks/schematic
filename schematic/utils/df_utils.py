import pandas as pd

def normalize_table(df: pd.DataFrame, primary_key:str) -> pd.DataFrame:
    
    """
    Function to normalize a table (e.g. dedup)
    Args:
        df: data frame to normalize
        primary_key: primary key on which to perform dedup

    Returns: a dedupped dataframe   
    """

    try:
        # if valid primary key has been provided normalize df
        df = df.reset_index()
        df_norm = df.drop_duplicates(subset = [primary_key])
        df_norm = df.drop(columns = ["index"])
        return df_norm
    except KeyError:
        # if the primary key is not in the df; then return the same df w/o changes
        print("Specified primary key is not in table schema. Proceeding without table changes.") 
        return df


def update_df(existing_df: pd.DataFrame, new_df: pd.DataFrame, idx_key: str) -> pd.DataFrame:
    """ Updates an existing data frame with the entries of another dataframe on a given primary index.

    Args: 
        existing_df : data frame to be updated
        new_df: data frame to update with
        idx_key: name of primary index column

    Returns: an updated data frame if the index column is present in both the existing and new data frames; ow returns the existing data frame w/o changes; the column set of the existing data frame are not updated (i.e. schema is preserved)
    """

    if not (idx_key in existing_df.columns and idx_key in new_df):
        return existing_df

    # merge the two data frames keeping all the resulting data in new and existing columns
    updated_df = pd.merge(existing_df, new_df, how = "outer", on = idx_key, suffixes = ("_existing", "_new"))

    # filter to keep only updated values across all columns in the schema
    for col in existing_df.columns:
        if col != idx_key and col != 'ROW_ID' and col != 'ROW_VERSION':
            existing_col = col + "_existing"
            new_col = col + "_new"

            updated_df[col] = updated_df[new_col]

            ###if updating using line below as an alternative to the current line only updates existing cells if the new cells are not null  
            #updated_df[col] = updated_df[existing_col].where(updated_df[new_col].isnull(), updated_df[new_col])


            
    # remove unnecessary columns and keep existing schema
    updated_df = updated_df[existing_df.columns]

    return updated_df
