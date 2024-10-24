"""SynapseDatabase"""

import pandas as pd
import synapseclient as sc  # type: ignore

from schematic.store.database.synapse_database_wrapper import Synapse
from schematic.store.synapse_tracker import SynapseEntityTracker


class SynapseDatabaseMissingTableAnnotationsError(Exception):
    """Raised when a table is missing expected annotations"""

    def __init__(self, message: str, table_name: str) -> None:
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}; " f"name: {self.table_name};"


class InputDataframeMissingColumn(Exception):
    """Raised when an input dataframe is missing a needed column(s)"""

    def __init__(
        self, message: str, table_columns: list[str], missing_columns: list[str]
    ) -> None:
        self.message = message
        self.table_columns = table_columns
        self.missing_columns = missing_columns
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"table_columns: {self.table_columns}; "
            f"missing_columns: {self.missing_columns}"
        )


class SynapseDatabase:
    """Represents a database stored as Synapse tables"""

    def __init__(
        self,
        auth_token: str,
        project_id: str,
        synapse_entity_tracker: SynapseEntityTracker = None,
        syn: sc.Synapse = None,
    ) -> None:
        """Init

        Args:
            auth_token (str): A Synapse auth_token
            project_id (str): A Synapse id for a project
            synapse_entity_tracker: Tracker for a pull-through cache of Synapse entities
        """
        self.synapse = Synapse(
            auth_token=auth_token,
            project_id=project_id,
            synapse_entity_tracker=synapse_entity_tracker,
            syn=syn,
        )

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Upserts rows into the given table

        Args:
            table_name (str): The name of the table to be upserted into.
            data (pd.DataFrame): The table the rows will come from

        Raises:
            SynapseDatabaseMissingTableAnnotationsError: Raised when the table has no
             primary key annotation.
        """
        table_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations = self.synapse.get_entity_annotations(table_id)
        if "primary_key" not in annotations:
            raise SynapseDatabaseMissingTableAnnotationsError(
                "Table has no primary_key annotation", table_name
            )
        primary_key = annotations["primary_key"][0]
        self._upsert_table_rows(table_id, data, primary_key)

    def _upsert_table_rows(
        self, table_id: str, data: pd.DataFrame, primary_key: str
    ) -> None:
        """Upserts rows into the given table

        Args:
            table_id (str): The Synapse id of the table to be upserted into.
            data (pd.DataFrame): The table the rows will come from
            primary_key (str): The primary key of the table used to identify
              which rows to update

        Raises:
            InputDataframeMissingColumn: Raised when the input dataframe has
              no column that matches the primary key argument.
        """
        if primary_key not in list(data.columns):
            raise InputDataframeMissingColumn(
                "Input dataframe missing primary key column.",
                list(data.columns),
                [primary_key],
            )

        table = self._create_primary_key_table(table_id, primary_key)
        merged_table = pd.merge(
            data, table, how="left", on=primary_key, validate="one_to_one"
        )
        self.synapse.upsert_table_rows(table_id, merged_table)

    def _create_primary_key_table(
        self, table_id: str, primary_key: str
    ) -> pd.DataFrame:
        """Creates a dataframe with just the primary key of the table

        Args:
            table_id (str): The id of the table to query
            primary_key (str): The name of the primary key

        Returns:
            pd.DataFrame: The table in pandas.DataFrame form with the primary key, ROW_ID, and
             ROW_VERSION columns

        Raises:
            InputDataframeMissingColumn: Raised when the synapse table has no column that
              matches the primary key argument.
        """
        table = self.synapse.query_table(table_id, include_row_data=True)
        if primary_key not in list(table.columns):
            raise InputDataframeMissingColumn(
                "Synapse table missing primary key column",
                list(table.columns),
                [primary_key],
            )
        table = table[["ROW_ID", "ROW_VERSION", primary_key]]
        return table
