"""df utils"""

# pylint: disable=logging-fstring-interpolation

import logging
from copy import deepcopy
from time import perf_counter
from typing import Union, Any, Optional
from datetime import datetime
import dateparser as dp
import pandas as pd
import numpy as np
from pandarallel import pandarallel  # type: ignore

logger = logging.getLogger(__name__)


def load_df(
    file_path: str,
    preserve_raw_input: bool = True,
    data_model: bool = False,
    allow_na_values: bool = False,
    **load_args: Any,
) -> pd.DataFrame:
    """
    Universal function to load CSVs and return DataFrames
    Parses string entries to convert as appropriate to type int, float, and pandas timestamp
    Pandarallel is used for type inference for large manifests to improve performance

    Args:
        file_path (str): path of csv to open
        preserve_raw_input (bool, optional): If false, convert cell datatypes to an inferred type
        data_model (bool, optional): bool, indicates if importing a data model
        allow_na_values (bool, optional): If true, allow pd.NA values in the dataframe
        **load_args(dict): dict of key value pairs to be passed to the pd.read_csv function

    Raises:
        ValueError: When pd.read_csv on the file path doesn't return as dataframe

    Returns:
        pd.DataFrame: a processed dataframe for manifests or unprocessed df for data models and
      where indicated
    """
    # start performance timer
    t_load_df = perf_counter()

    # Read CSV to df as type specified in kwargs
    org_df = pd.read_csv(  # type: ignore
        file_path, keep_default_na=True, encoding="utf8", **load_args
    )
    if not isinstance(org_df, pd.DataFrame):
        raise ValueError(
            (
                "Pandas did not return a dataframe. "
                "Pandas will return a TextFileReader if chunksize parameter is used."
            )
        )

    # only trim if not data model csv
    if not data_model:
        org_df = trim_commas_df(org_df, allow_na_values=allow_na_values)

    if preserve_raw_input:
        logger.debug(f"Load Elapsed time {perf_counter()-t_load_df}")
        return org_df

    ints, is_int = find_and_convert_ints(org_df)

    float_df = convert_floats(org_df)

    # Store values that were converted to type int in the final dataframe
    processed_df = float_df.mask(is_int, other=ints)

    logger.debug(f"Load Elapsed time {perf_counter()-t_load_df}")
    return processed_df


def find_and_convert_ints(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find strings that represent integers and convert to type int
    Args:
        dataframe: dataframe with nulls masked as empty strings
    Returns:
        ints: dataframe with values that were converted to type int
        is_int: dataframe with boolean values indicating which cells were converted to type int

    """
    # pylint: disable=unnecessary-lambda
    large_manifest_cutoff_size = 1000
    # Find integers stored as strings and replace with entries of type np.int64
    if (
        dataframe.size < large_manifest_cutoff_size
    ):  # If small manifest, iterate as normal for improved performance
        ints = dataframe.map(  # type:ignore
            lambda cell: convert_ints(cell), na_action="ignore"
        ).fillna(False)

    else:  # parallelize iterations for large manifests
        pandarallel.initialize(verbose=1)
        ints = dataframe.parallel_applymap(  # type:ignore
            lambda cell: convert_ints(cell), na_action="ignore"
        ).fillna(False)

    # Identify cells converted to integers
    is_int = ints.map(pd.api.types.is_integer)  # type:ignore

    assert isinstance(ints, pd.DataFrame)
    assert isinstance(is_int, pd.DataFrame)

    return ints, is_int


def convert_ints(string: str) -> Union[np.int64, bool]:
    """
    Lambda function to convert a string to an integer if possible, otherwise returns False
    Args:
        string: string to attempt conversion to int
    Returns:
        string converted to type int if possible, otherwise False
    """
    if isinstance(string, str) and str.isdigit(string):
        return np.int64(string)
    return False


def convert_floats(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Convert strings that represent floats to type float
    Args:
        dataframe: dataframe with nulls masked as empty strings
    Returns:
        float_df: dataframe with values that were converted to type float. Columns are type object
    """
    # create a separate copy of the manifest
    # before beginning conversions to store float values
    float_df = deepcopy(dataframe)

    # convert strings to numerical dtype (float) if possible, preserve non-numerical strings
    for col in dataframe.columns:
        float_df[col] = pd.to_numeric(float_df[col], errors="coerce").astype("object")

        # replace values that couldn't be converted to float with the original str values
        float_df[col].fillna(dataframe[col][float_df[col].isna()], inplace=True)

    return float_df


def parse_dates(date_string: str) -> Union[datetime, bool]:
    """Gets a datetime from a string

    Args:
        date_string (str): The string to get the datetime from

    Returns:
        Union[datetime, bool]: The parsed datetime or False
    """
    try:
        date = dp.parse(date_string=date_string, settings={"STRICT_PARSING": True})
        return date if date else False
    except TypeError:
        return False


def normalize_table(dataframe: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    """
    Function to normalize a table (e.g. dedup)
    Args:
        dataframe: data frame to normalize
        primary_key: primary key on which to perform dedup

    Returns: a dedupped dataframe
    """

    try:
        # if valid primary key has been provided normalize df
        dataframe = dataframe.reset_index()
        df_norm = dataframe.drop_duplicates(subset=[primary_key])
        df_norm = dataframe.drop(columns=["index"])
        return df_norm
    except KeyError:
        # if the primary key is not in the df; then return the same df w/o changes
        logger.warning(
            "Specified primary key is not in table schema. Proceeding without table changes."
        )

        return dataframe


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


def trim_commas_df(
    dataframe: pd.DataFrame,
    allow_na_values: Optional[bool] = False,
) -> pd.DataFrame:
    """Removes empty (trailing) columns and empty rows from pandas dataframe (manifest data).

    Args:
        dataframe: pandas dataframe with data from manifest file.
        allow_na_values (bool, optional): If true, allow pd.NA values in the dataframe

    Returns:
        df: cleaned-up pandas dataframe.
    """
    # remove all columns which have substring "Unnamed" in them
    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("^Unnamed")]

    # remove all completely empty rows
    dataframe = dataframe.dropna(how="all", axis=0)

    if allow_na_values is False:
        # Fill in nan cells with empty strings
        dataframe.fillna("", inplace=True)
    return dataframe


def col_in_dataframe(col: str, dataframe: pd.DataFrame) -> bool:
    """Check if a column is in a dataframe, without worrying about case

    Args:
        col: name of column whose presence in the dataframe is being checked
        dataframe: pandas dataframe with data from manifest file.

    Returns:
        bool: whether or not the column name is a column in the dataframe, case agnostic
    """
    return col.lower() in [
        manifest_col.lower() for manifest_col in dataframe.columns.to_list()
    ]


def populate_df_col_with_another_col(
    dataframe: pd.DataFrame, source_col: str, target_col: str
) -> pd.DataFrame:
    """Copy the values from one column in a dataframe to another column in the same dataframe
    Args:
        dataframe: pandas dataframe with data from manifest file.
        source_col: column whose contents to copy over
        target_col: column to be updated with other contents

    Returns:
        dataframe with contents updated
    """
    # Copy the contents over
    dataframe[target_col] = dataframe[source_col]
    return dataframe
