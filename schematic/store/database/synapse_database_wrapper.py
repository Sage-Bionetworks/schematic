"""Wrapper class for interacting with Synapse database objects. Eventually this will 
be replaced with a more database/table class that exists within the SYNPY project."""

from typing import Optional

import pandas  # type: ignore
import synapseclient  # type: ignore
from opentelemetry import trace

from schematic.store.synapse_tracker import SynapseEntityTracker


class SynapseTableNameError(Exception):
    """SynapseTableNameError"""

    def __init__(self, message: str, table_name: str) -> None:
        """
        Args:
            message (str): A message describing the error
            table_name (str): The name of the table
        """
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}:{self.table_name}"


class Synapse:  # pylint: disable=too-many-public-methods
    """
    The Synapse class handles interactions with a project in Synapse.
    """

    def __init__(
        self,
        auth_token: str,
        project_id: str,
        cache_root_dir: Optional[str] = None,
        synapse_entity_tracker: SynapseEntityTracker = None,
        syn: synapseclient.Synapse = None,
    ) -> None:
        """Init

        Args:
            auth_token (str): A Synapse auth_token
            project_id (str): A Synapse id for a project
            cache_root_dir( str ): Where the directory of the synapse cache should be located
            synapse_entity_tracker: Tracker for a pull-through cache of Synapse entities
        """
        self.project_id = project_id
        if syn:
            self.syn = syn
        else:
            syn = synapseclient.Synapse(cache_root_dir=cache_root_dir)
            syn.login(authToken=auth_token, silent=True)
            current_span = trace.get_current_span()
            if current_span.is_recording():
                current_span.set_attribute("user.id", syn.credentials.owner_id)
            self.syn = syn
        self.synapse_entity_tracker = synapse_entity_tracker or SynapseEntityTracker()

    def get_synapse_id_from_table_name(self, table_name: str) -> str:
        """Gets the synapse id from the table name

        Args:
            table_name (str): The name of the table

        Raises:
            SynapseTableNameError: When no tables match the name
            SynapseTableNameError: When multiple tables match the name

        Returns:
            str: A synapse id
        """
        matching_table_id = self.syn.findEntityId(
            name=table_name, parent=self.project_id
        )
        if matching_table_id is None:
            raise SynapseTableNameError("No matching tables with name:", table_name)
        return matching_table_id

    def query_table(
        self, synapse_id: str, include_row_data: bool = False
    ) -> pandas.DataFrame:
        """Queries a whole table

        Args:
            synapse_id (str): The Synapse id of the table to delete
            include_row_data (bool): Include row_id and row_etag. Defaults to False.

        Returns:
            pandas.DataFrame: The queried table
        """
        query = f"SELECT * FROM {synapse_id}"
        return self.execute_sql_query(query, include_row_data)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pandas.DataFrame:
        """Execute a Sql query

        Args:
            query (str): A SQL statement that can be run by Synapse
            include_row_data (bool): Include row_id and row_etag. Defaults to False.

        Returns:
            pandas.DataFrame: The queried table
        """
        result = self.execute_sql_statement(query, include_row_data)
        table = pandas.read_csv(result.filepath)
        return table

    def execute_sql_statement(
        self, statement: str, include_row_data: bool = False
    ) -> synapseclient.table.CsvFileTable:
        """Execute a SQL statement

        Args:
            statement (str): A SQL statement that can be run by Synapse
            include_row_data (bool): Include row_id and row_etag. Defaults to False.

        Returns:
            synapseclient.table.CsvFileTable: The synapse table result from
              the provided statement
        """
        table = self.syn.tableQuery(
            statement, includeRowIdAndRowVersion=include_row_data
        )
        assert isinstance(table, synapseclient.table.CsvFileTable)
        return table

    def upsert_table_rows(self, synapse_id: str, data: pandas.DataFrame) -> None:
        """Upserts rows from  the given table

        Args:
            synapse_id (str): The Synapse ID fo the table to be upserted into
            data (pandas.DataFrame): The table the rows will come from
        """
        self.syn.store(synapseclient.Table(synapse_id, data))
        # Commented out until https://sagebionetworks.jira.com/browse/PLFM-8605 is resolved
        # storage_result = self.syn.store(synapseclient.Table(synapse_id, data))
        # self.synapse_entity_tracker.add(synapse_id=storage_result.schema.id, entity=storage_result.schema)
        self.synapse_entity_tracker.remove(synapse_id=synapse_id)

    def get_entity_annotations(self, synapse_id: str) -> synapseclient.Annotations:
        """Gets the annotations for the Synapse entity

        Args:
            synapse_id (str): The Synapse id of the entity

        Returns:
            synapseclient.Annotations: The annotations of the Synapse entity in dict form.
        """
        entity = self.synapse_entity_tracker.get(
            synapse_id=synapse_id, syn=self.syn, download_file=False
        )
        return synapseclient.Annotations(
            id=entity.id, etag=entity.etag, values=entity.annotations
        )
