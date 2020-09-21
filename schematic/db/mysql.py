from typing import Any, Dict, Optional, Text, List, TextIO

import pandas as pd

from rdb import RDB

class MySQL(object):

    def __init__(self,
                 input_table: pd.DataFFrame,
            ) -> None:

        """Instantiate object of type MySQL. Includes method to serialize
           a simple relation database model as a MySQL schema. Includes utility
           methods to automatically populate a SQL database given a (potentially denormalized table) (pandas df).
           The update converts the denormalized table into a set of normalized schema tables (pandas df); it 
           validates that each normalized table matches the config metadata model json-ld schema. 
           If valid, the normalized tables are then ready to load into a sql database (a csv dump method 
           and MySQL csv load statements could be generated).

           Args: 
                input_table: a pandas dataframe that may contain a subset of columns across all
                tables in the db schema; the input may be denormalized
        """

        self.input_table = input_table 
        self.rdb = RDB()

    def create_table_sql(table_label:str) -> str:
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


    def create_db_tables() -> str:
        """ Create a dump of MySQL table schemas;

            Returns:
                A string MySQL dump creating all tables in the RDB schema 
        """

        sql_dump = ""

        for table_label in self.rdb.tables.keys():
            sql_dump += create_table_sql(table_label) 
        
        return sql_dump
        

    def populate_sql_tables(self):
        """ Given the input table dataframe update all necessary SQL tables
        """
        # get the target update tables based on input table attributes
        # note that this assumes columns in the input table will have the same names
        # as matching rdb schema table column names (i.e. corresponding property labels in json-ld schema)
        target_tables = self.rdb.get_target_update_tables(input_table.columns)

    
        table_schema = db_schema["tables"][table_name]
        sub_sop_columns = list(set(table_schema.keys()).difference(set(db_schema["ignore_attributes"])))
         
        # get sub-SOP columns from update data frame if they exist
        df.loc[:, df.columns.isin(list('BCD'))]
        df_sub_sop = df_update[sub_sop_columns]




        # for each table in the target update tables
        # fill in columns that are missing in the provided input 
        # (that's ok; e.g. not all entity attributes may be required in the schema)
        for 
        

        # perform joins if needed based on foreign keys and rdb schema graph
        # (the table that we are joining have been updated already if needed)
        #
        # normalize table on primary key, if needed
        #
        # validate resulting table wrt json-ld schema
