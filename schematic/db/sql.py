from typing import Any, Dict, Optional, Text, List, TextIO
import logging

import pandas as pd
import numpy as np

import sqlalchemy as sqla
from sqlalchemy import  Table, Column, Text, Integer, String, ForeignKey
from sqlalchemy_utils import database_exists, create_database

from schematic.db.rdb import RDB
import schematic.utils.df_utils

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class SQL(object):

    def __init__(self,
            rdb: RDB,
            connection: str 
            ) -> None:

        """Instantiate a SQL object. Includes method to 
           - create a SQL database given a RDB instance and DB connection 
           - create tables in the SQL database
           - populate tables with data from a dataframe 
           Uses sqlalchemy as a universal SQL DB interface
        
           Args:
                rdb: an instance of schematic RDB object
                connection: a SQL alchemy connection (https://docs.sqlalchemy.org/en/13/core/connections.html); e.g.
                connection = str("mysql://{0}:{1}@{2}/".format(username, password, host))
                where username, password and host are variables containing the mysql DB 
                username, password and host address (e.g. localhost) 
           Returns:
                None
        """

        self.rdb = rdb
        self.connection = connection

        self.schema_name = self.rdb.schema_name

        # create a sqlalchemy engine that handles all interactions with the DB specified in connection
        self.engine = sqla.create_engine(connection, encoding = 'utf-8', echo = True)
        logger.debug("Successfully created SQL engine")

        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        else:
            self.engine.connect()

        # create a sqlalchemy metadata object to hold all sqlalchemy DB objects
        self.metadata = sqla.MetaData()

        # instantiate a SQL DB
        self.create_sql_db()


    def create_sql_db(self) -> None:
        """ Create a SQL DB schema, if a DB schema with this name doesn't exist

            Args:
                None
            Returns:
                None
        """
        create_query = str("CREATE SCHEMA IF NOT EXISTS {0};".format(self.schema_name))
        self.engine.execute(create_query)
        logger.debug("Successfully instantiated a SQL DB with schema name: " + self.schema_name)


    def create_table_sqla(self, table_label:str) -> str:
        """ Given a table label in the RDB schema generate a sqlalchemy table object

        Args:
            table_label: a RDB-schema table label

        Returns:
            None
        """
    
        # generate sqlalchemy column objects
        columns = []

        for attr, attr_schema in self.rdb.tables[table_label]['attributes'].items():

            # by default, create a column of type Text
            #TODO: get column type from RDB model
            col = Column(attr, Text)

            # if this attribute is the PK for this table, set as PK
            if attr == self.rdb.tables[table_label]['primary_key']:
                col = Column(attr, String(128), primary_key = True)
            
            # if this attribute is in the set of FKs for this table, set as FK
            if attr in self.rdb.tables[table_label]['foreign_keys']:
                col = Column(attr, String(128), ForeignKey(attr))
            
            columns.append(col)

        table_sql = Table(table_label, self.metadata, *columns)
        
        logger.debug("Successfully added table " + table_label + " to sqlalchemy metadata model")


    def create_db_tables(self) -> None:
        """ Create all DB tables

            Returns:
                None
        """

        # for each table in the RDB layer, create a sqlalchemy table object
        for table_label in self.rdb.tables.keys():
            self.create_table_sqla(table_label) 
       
        # create all tables (if not existing)
        self.metadata.create_all(self.engine)

    
   

    def update_table_sqla(self, table_label: str, update_table: pd.DataFrame) -> bool:
        """ Given a normalized table data frame, update corresponding DB table indicated by table_label.
        For each row to be updated; only insert a row if no primary key duplicates; ow update existing keys.
        This is an upsert operation; 
        e.g. https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-antipatterns/

        TODO: handle integrity errors; current implementation do not account for that:
        e.g. https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-integrityerror.html
        We assume normalized table

        Args:
            table_label: target table to update
            update_table: a data frame containing table updates

        Returns:
            If table update completes, return True; ow return False
        """

        return True


    def replace_table_sqla(self, table_label: str, update_table: pd.DataFrame) -> str:

        """ Given a normalized table data frame, update corresponding DB table indicated by table_label.
        This is a *replace* operation: the existing table is dropped and replaced by the data in the update_table

        Args:
            table_label: target table to update
            update_table: a data frame containing table updates

        Returns:
            If table update completes, return True; ow return False
        """
        
        update_table.to_sql(table_label, self.engine, if_exists = 'replace')

        return table_label


    def replace_db_tables(self, input_table:pd.DataFrame, validate: bool = False, full_validation: bool = False) -> List[str]: 
        """ Given an input table dataframe, that may be a denormalized view of data across multiple DB tables, 
        generate a set of normalized dataframes and update the corresponding DB tables 
        by *replacing* them with data in the dataframe.
         

        Args:
            input_table: a dataframe containing DB updates
            validate: if True, validate each table update matches the metadata model schema for that table 
            only update a table if the validation passes for that table; if False update tables 
            and do not validate (e.g. validation can be done later on the entire table row set)
            full_validation: if True, validate each table update matches the metadata model schema for 
            that table - do not update any table if any of the update table does not validate successfully
        Returns:
            A list of table labels for tables that were successfully updated.
        """

        # convert column names from display names to property labels
        dn_pl = self.rdb.get_property_labels_from_table_attrs(input_table.columns)
        input_table = input_table.rename(columns = dn_pl)

        # get the target update tables based on input table attributes
        # note that this assumes columns in the input table will have the same names
        # as matching rdb schema table column names (i.e. corresponding property labels in json-ld schema)
        target_tables = self.rdb.get_target_update_tables(input_table.columns)

        # for each table in the target update set of tables, create the corresponding normalized 
        # update data frame
        
        updated_tables = []
        for table_label in target_tables:

            # get table attributes based on schema
            table_attributes = list(self.rdb.tables[table_label]['attributes'].keys())
        
            # get the relevant set of attribute columns from update data frame if they exist
            update_table = input_table[input_table.columns & table_attributes]

            # the update table might not have all columns in the table schema
            # fill the missing columns w/ nulls
            for missing_column in set(table_attributes).difference(update_table.columns):
                update_table[missing_column] = None

            # normalize update table
            update_table = df_utils.normalize_table(update_table, self.rdb.tables[table_label]['primary_key'])

            if validate:
                pass # TODO: call json schema validator on update_table; do not generate table update statement if table doesn't match the schema
            if full_validate:
                pass # TODO: change code logic to validate tableas and do not generate any table update statements if even a single table fails validation

            # get sql table update query
            if not self.update_table_sqla(table_label, update_table):
                updated_tables.append(table_label)
        
        return updated_tables
