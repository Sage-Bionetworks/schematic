from typing import Any, Dict, Optional, Text, List, TextIO

import pandas as pd
import numpy as np

import schematic.utils.df_utils

from rdb import RDB

class MySQL(object):

    def __init__(self,
            ) -> None:

        """Instantiate object of type MySQL. Includes method to serialize
           a simple relation database model as a MySQL schema. Includes utility
           methods to automatically populate a SQL database given a (potentially denormalized table) (pandas df).

           Args:
                None
           Returns:
                None
        """

        self.rdb = RDB()


    def create_table_sql(self, table_label:str) -> str:
        """ Given a table label in the RDB schema generate a MySQL table create statement

        Args:
            table_label: a RDB-schema table label

        Returns:
            A SQL statement string
        """

        table_sql = "CREATE TABLE " + table_label + "\n"\
                           + "(" + "\n"

        for attr, attr_schema in self.rdb.tables[table_label]['attributes'].items():
            table_sql += attr + " " + attr_schema['type'] + ",\n" 
        
        table_sql += "PRIMARY KEY (" + self.rdb.tables[table_label]['primary_key'] + ")\n"\
                        + ");\n\n"

        return table_sql

    def update_table_sql(self, update_table: pd.DataFrame) -> str:

        """ Given an update table dataframe generate a MySQL table update query
        for each row to be updated; only insert a row if no primary key duplicates; ow update existing keys 

        Args:
            update_table: a data frame containing table updates

        Returns:
            A SQL statement string
        """

        update_sql = ""

        return update_sql


    def create_db_tables(self) -> str:
        """ Create a dump of MySQL table schemas;

            Returns:
                A string MySQL dump creating all tables in the RDB schema 
        """

        sql_dump = ""

        for table_label in self.rdb.tables.keys():
            sql_dump += self.create_table_sql(table_label) 
        
        return sql_dump
        

    def update_db_tables(self, input_table:pd.DataFrame, validate = False, full_validate = False) -> str: 
        """ Given an input table dataframe, generate a MySQL dump of 
        SQL table update commands for each affected table 

        Args:
            input_table: a dataframe containing DB updates
            validate: if True, validate each table update matches the metadata model schema for that table 
            only update a table if the validation passes for that table; if False update tables 
            and do not validate (e.g. validation can be done later on the entire table row set)
            full_validate: if True, validate each table update matches the metadata model schema for 
            that table - do not update any table if any of the update table does not validate successfully
        Returns:
            A string MySQL dump updating existing tables; insert a new row in a given table,
            unless the primary key exists, in which case update the table 
        """

        tables_update_sql = ""

        # get the target update tables based on input table attributes
        # note that this assumes columns in the input table will have the same names
        # as matching rdb schema table column names (i.e. corresponding property labels in json-ld schema)
        target_tables = self.rdb.get_target_update_tables(input_table.columns)

        # for each table in the target update set of tables, create the corresponding normalized 
        # update data frame
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
            table_updates_sql += self.update_table_sql(update_table)
