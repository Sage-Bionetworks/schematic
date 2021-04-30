import logging

import pandas as pd


logger = logging.getLogger(__name__)


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
    return df
