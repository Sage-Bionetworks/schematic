import logging
from copy import deepcopy
from time import perf_counter
import datetime as dt
import dateparser as dp
import pandas as pd
import numpy as np
from pandarallel import pandarallel

logger = logging.getLogger(__name__)


def load_df(file_path, preserve_raw_input=True, data_model=False, **load_args):
    """
    Universal function to load CSVs and return DataFrames
    Parses string entries to convert as appropriate to type int, float, and pandas timestamp
    Pandarallel is used for type inference for large manfiests to improve performance
    Args:
        file_path: path of csv to open
        preserve_raw_input: Bool. If false, convert cell datatypes to an inferred type
        data_model: bool, indicates if importing a data model
        load_args: dict of key value pairs to be passed to the pd.read_csv function
        **kwargs: keyword arguments for pd.read_csv()

    Returns: a processed dataframe for manifests or unprocessed df for data models and where indicated
    """
    large_manifest_cutoff_size = 1000
    # start performance timer
    t_load_df = perf_counter()
    
    #Read CSV to df as type specified in kwargs
    org_df = pd.read_csv(file_path, keep_default_na = True, encoding='utf8', **load_args)
    
    # If type inference not allowed: trim and return
    if preserve_raw_input:
        #only trim if not data model csv
        if not data_model:
            org_df=trim_commas_df(org_df)
        
            # log manifest load and processing time
            logger.debug(f"Load Elapsed time {perf_counter()-t_load_df}")
        return org_df

    # If type inferences is allowed: infer types, trim, and return
    else:
        # create a separate copy of the manifest 
        # before beginning conversions to store float values
        float_df=deepcopy(org_df)
        
        # Cast the columns in the dataframe to string and
        # replace Null values with empty strings
        null_cells = org_df.isnull() 
        org_df = org_df.astype(str).mask(null_cells, '')

        # Find integers stored as strings and replace with entries of type np.int64
        if org_df.size < large_manifest_cutoff_size:  # If small manifest, iterate as normal for improved performance
            ints = org_df.applymap(lambda x: np.int64(x) if str.isdigit(x) else False, na_action='ignore').fillna(False)

        else:   # parallelize iterations for large manfiests
            pandarallel.initialize(verbose = 1)
            ints = org_df.parallel_applymap(lambda x: np.int64(x) if str.isdigit(x) else False, na_action='ignore').fillna(False)

        # convert strings to numerical dtype (float) if possible, preserve non-numerical strings
        for col in org_df.columns:
            float_df[col]=pd.to_numeric(float_df[col], errors='coerce')
            # replace values that couldn't be converted to float with the original str values
            float_df[col].fillna(org_df[col][float_df[col].isna()],inplace=True)
        
        # Trim nans and empty rows and columns
        processed_df = trim_commas_df(float_df)
        
        # Store values that were converted to type int in the final dataframe
        processed_df=processed_df.mask(ints != False, other = ints)  
        
        # log manifest load and processing time
        logger.debug(f"Load Elapsed time {perf_counter()-t_load_df}")
        return processed_df


def _parse_dates(date_string):
    try:
        date = dp.parse(date_string = date_string, settings = {'STRICT_PARSING': True})
        return date if date else False
    except TypeError:
        return False



def normalize_table(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:

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
        df_norm = df.drop_duplicates(subset=[primary_key])
        df_norm = df.drop(columns=["index"])
        return df_norm
    except KeyError:
        # if the primary key is not in the df; then return the same df w/o changes
        logger.warning(
            "Specified primary key is not in table schema. Proceeding without table changes."
        )

        return df


def update_df(
    input_df: pd.DataFrame, updates_df: pd.DataFrame, index_col: str = "entityId"
) -> pd.DataFrame:
    """Update a manifest using another data frame with Synapse IDs.

    The input `input_df` is copied to avoid changing the input.

    Both input data frames must have an `entityId` column. Any rows
    in `updates_df` corresponding to entities that don't appear in
    `input_df` are silently dropped. Similarly, any columns in
    `updates_df` that don't appear in `input_df` are not added.

    IMPORTANT: This function is currently designed to handle empty
    manifests because it will not raise an error or warning if any
    overwriting of existing values takes place.

    TODO: Handle conflicts/overwriting more elegantly. See:
    https://github.com/Sage-Bionetworks/schematic/issues/312#issuecomment-725750931

    Args:
        input_df (pd.DataFrame): Manifest data frame. Must
        include the `index_col` column.
        updates_df (pd.DataFrame): Data frame with updates. Must
        include the `index_col` column. This data frame doesn't
        need to include all of the column names from `input_df`.
        index_col (str): Column to index the data frames on.

    Returns:
        pd.DataFrame: Updated `input_df` data frame.
    """
    # Confirm that entityId is present in both data frames
    assert index_col in input_df, f"`input_df` lacks `{index_col}` column."
    assert index_col in updates_df, f"`updates_df` lacks `{index_col}` column."

    # Set `inplace=False` to copy input data frames and avoid side-effects
    input_df_idx = input_df.set_index(index_col, inplace=False)
    updates_df_idx = updates_df.set_index(index_col, inplace=False)

    # Update manifest data frame and reset index
    input_df_idx.update(updates_df_idx, overwrite=True)

    # Undo index and ensure original column order
    input_df_idx.reset_index(inplace=True)
    input_df_idx = input_df_idx[input_df.columns]

     # Sometimes pandas update can change the column datatype, recast
    for col in input_df_idx.columns:
        input_df_idx[col] = input_df_idx[col].astype(input_df.dtypes[col])

    return input_df_idx


def trim_commas_df(df: pd.DataFrame):
    """Removes empty (trailing) columns and empty rows from pandas dataframe (manifest data).

    Args:
        df: pandas dataframe with data from manifest file.

    Returns:
        df: cleaned-up pandas dataframe.
    """
    # remove all columns which have substring "Unnamed" in them
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # remove all completely empty rows
    df = df.dropna(how="all", axis=0)

    #Fill in nan cells with empty strings
    df.fillna("", inplace=True)
    return df


def col_in_dataframe(col: str, df: pd.DataFrame) -> bool:
    """Check if a column is in a dataframe, without worring about case

    Args:
        col: name of column whose presence in the dataframe is being checked
        df: pandas dataframe with data from manifest file.

    Returns:
        bool: whether or not the column name is a column in the dataframe, case agnostic
    """
    return col.lower() in [manifest_col.lower() for manifest_col in df.columns.to_list()]

def populate_df_col_with_another_col(df: pd.DataFrame, source_col: str, target_col: str) -> pd.DataFrame:
    """Copy the values from one column in a dataframe to another column in the same dataframe
    Args:
        df: pandas dataframe with data from manifest file.
        source_col: column whose contents to copy over
        target_col: column to be updated with other contents

    Returns:
        dataframe with contents updated
    """
    # Copy the contents over
    df[target_col]=df[source_col]
    return df