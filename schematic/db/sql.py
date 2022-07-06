from datetime import datetime, timezone
from typing import Any, Dict, Optional, Text, List, TextIO
import logging

import os
import pandas as pd
import numpy as np

import networkx as nx
from schematic.utils.viz_utils import visualize

import sqlalchemy as sa
from sqlalchemy import  Table, Column, Text, Integer, String, ForeignKey, ForeignKeyConstraint, inspect
from sqlalchemy.ext.automap import automap_base, generate_relationship, name_for_collection_relationship
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_schemadisplay import create_schema_graph, create_uml_graph
from sqlalchemy.orm import interfaces
from sqlalchemy.orm import relationship, backref

import time

from schematic import schemas
from schematic.schemas.explorer import SchemaExplorer
from schematic.schemas.generator import SchemaGenerator


from schematic.db.rdb import RDB
from schematic.utils import df_utils

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
        self.engine = sa.create_engine(connection, encoding = 'utf-8', echo = True)
        logger.debug("Successfully created SQL engine")

        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        else:
            self.engine.connect()
        
        logger.debug("Successfully instantiated a SQL DB with schema name: " + self.schema_name)

        # create a sqlalchemy metadata object to hold all sqlalchemy DB objects
        self.metadata = sa.MetaData()

        #MD
        # get metadata model schema graph
        self.mm_graph = self.rdb.sg.se.get_nx_schema()
        self.sg = self.rdb.sg


    def create_db_sa(self) -> None:
        """ Create a SQL DB schema, if a DB schema with this name doesn't exist

            Args:
                None
            Returns:
                None
        """
        create_query = str("CREATE SCHEMA IF NOT EXISTS {0};".format(self.schema_name))
        self.engine.execute(create_query)

    def table_and_fk_attr_match(self, fk):
        """ Find if the table and attribute names match. 
            If they dont it would imply the foriegn key 
            has a different name than the primary key.
            Args:
                fk: str, foreign key containing table prefix.
            Returns: 
                bool, True if the table and id match.
        """
        target_table = fk.split('.')[0].lower()
        target_attr = fk.split('.')[1].lower().replace('id', '')

        return target_table == target_attr

    def find_matched_tables_fks(self):
        """ When a FK and PK do not share naming get the table 
            where the PK is located, along with corresponding FKs.
            Args:
                None
            Returns:
                pk_tables: list of strings, primary key tables.
                fks: list of strings, foreign keys

        """
        pk_table = []
        fks = []
        for table_label in self.rdb.tables.keys():
            for fk in self.rdb.tables[table_label]['foreign_keys']:
                if not self.table_and_fk_attr_match(fk):
                    pk_table.append(fk.split('.')[0])
                    fks.append(fk)
        pk_table_set = set(pk_table)
        pk_tables = list(pk_table_set)
        return pk_tables, fks

    def reverse_dict_with_list(self, d):
        r_dict = {}
        for key, value in d.items():
            for v in value:
                if v not in r_dict.keys():
                    r_dict[v] = []
                r_dict[v].append(key)
        return r_dict

    def _find_child_parent_relationships(self):
        # Refers to sql alchemy parent child relationships.
        # First for each table, track the tables where the foreign keys end up
        
        child_to_parent_table_dict = {}
        for table_label in self.rdb.tables.keys():
            fks = self.rdb.tables[table_label]['foreign_keys']
            child_to_parent_table_dict[table_label] = [fk.split('.')[0] for fk in fks]
        parent_to_child_table_dict = self.reverse_dict_with_list(child_to_parent_table_dict)
        return child_to_parent_table_dict, parent_to_child_table_dict

    def create_table_sa(self, table_label:str) -> str:
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
            if not attr in self.rdb.get_table_foreign_keys(table_label, table_prefix = False):
                col = Column(attr, Text)
                columns.append(col)

        # if the table is resources add a UNIX Time Column.
        # TODO: Move this to the data model, then hide it from the manifest.
        '''
        if table_label == 'Resource':
            col = Column('updateTimeUnix', Text)
            columns.append(col)
        '''
        # set PK 
        pk = self.rdb.tables[table_label]['primary_key']
        col = Column(pk, String(128), primary_key = True, autoincrement = False, nullable = False)
        columns.append(col)
        
        # set FKs 
        for fk in self.rdb.tables[table_label]['foreign_keys']:
            #Infer how a FK should attach to a PK based on its naming.
            target_table = fk.split('.')[0]
            # If the FK has a different name than the PK establish a
            # relationship based on the tables that are being connected.
            if not self.table_and_fk_attr_match(fk):
                # FK is in a different table than the PK
                if target_table != table_label:
                    pk_attr = self.rdb.tables[target_table]['primary_key']
                    fk_attr = self.rdb.get_attr_from_fk(fk)
                    primary_key = target_table + '.' + pk_attr
                    pk_id = Column(pk_attr, ForeignKey(primary_key))
                    fk_id = Column(fk_attr, ForeignKey(primary_key))
                    target_relationship = relationship(target_table, foreign_keys=[pk_id])
                    current_relationship = relationship(target_table, foreign_keys=[fk_id])
                    columns.append(pk_id)
                    columns.append(fk_id)
                # FK is in the same table as the PK
                elif target_table == table_label:
                    pk_attr = self.rdb.tables[target_table]['primary_key']
                    fk_attr = self.rdb.get_attr_from_fk(fk)
                    primary_key = target_table + '.' + pk_attr
                    fk_id = Column(fk_attr, ForeignKey(primary_key))
                    relshp = relationship(target_table, foreign_keys=[fk_id])
                    columns.append(fk_id)
            # If FK and PKs match.
            else:
                fk_attr = self.rdb.get_attr_from_fk(fk)
                col = Column(fk_attr, String(128), ForeignKey(fk), nullable=True)
                columns.append(col)
                #col = ForeignKeyConstraint([fk_attr], [fk])
                #columns.append(col)

        table_sql = Table(table_label, self.metadata, *columns)
        logger.debug("Successfully added table " + table_label + " to sqlalchemy metadata model")


    def create_db_tables(self) -> None:
        """ Create all DB tables

            Returns:
                None
        """
    
        pk_tables, fks = self.find_matched_tables_fks()
        # For each table in the RDB layer, create a sqlalchemy table object
        # Create SA table first for tables where there are primary keys with 
        # many relationships (so that they can be referenced by other tables)
        for table in pk_tables:
            self.create_table_sa(table)

        for table_label in self.rdb.tables.keys():
            if table_label not in pk_tables:
                self.create_table_sa(table_label)

        # create all tables (if not existing)
        self.metadata.create_all(self.engine)

    def update_table_sa(self, table_label: str, update_table: pd.DataFrame, dialect: str = 'mysql') -> str:
        """ Given a normalized table data frame, update corresponding DB table indicated by table_label.
        For each row to be updated; only insert a row if no primary key duplicates; ow update existing keys.
        This is an upsert operation; 
        e.g. https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-antipatterns/

        We assume normalized table

        TODO: support other dialects than mysql (e.g. postgresql)
        TODO: this implementation is not elegant and efficient; make more elegant 
        (e.g. handle various dialects) and efficient (e.g. avoid explicit iteration over rows)

        Args:
            table_label: target table to update
            update_table: a data frame containing table updates
            dialect: SQL database engine

        Returns:
            If table update completes, return table_label
        """

        if dialect == "mysql":
            # generate an upsert query for a mysql db
                        
            # reformat columns names for SQL query INSERT part
            sep = ", "
            columns_isqlf = "(" + sep.join([""+c+"" for c in update_table.columns]) + ")"

            # reformat columns names for SQL query UPDATE part 
            columns_usqlf = sep.join([c+"=%s" for c in update_table.columns])

            # format values placeholders part of query
            columns_vsqlf = "(" + sep.join(["%s" for c in update_table.columns]) + ")"
            
            # change NaN values to ""
            update_table = update_table.fillna("NaN")
            #update_table = update_table.fillna(None)

            # get row data 
            rows = update_table.to_dict('split')["data"]

            # upsert rows one by one
            for row in rows:
                # assemble query
                query = "INSERT INTO `" + table_label + "` " + columns_isqlf +\
                        " VALUES " + columns_vsqlf +\
                        " ON DUPLICATE KEY UPDATE " + columns_usqlf
                try:
                    # execute query; note we have to pass row twice to fill in all format strings 
                    self.engine.execute(query, row + row)
                except:
                    row = [None if x is 'NaN' or x is '\n' else x for x in row]                
                    self.engine.execute(query, row + row)

        return table_label

    def check_db_columns(self, table_label: str, column_name: str) -> str:
        """ 
        """

        query = "SELECT {}{}{} FROM {}{}{}".format("`", column_name, "`", "`",table_label, "`")
        column_value = self.run_sql_query(self.engine, [query])
        if column_value.empty:
            cols_present = False
        else:
            cols_present = True
        return cols_present

    def get_updated_rows(self, table_label, update_table, pk):
        '''
        Do a sql query for current table.
        Compare to new table.
        Find which rows are being updated and get the primary key ID.
        
        TODO: This part is very specific to how the NF database function and needs to be abstracted
        out in some way

        Returns a dictionary of rows that are updated 
        '''
        query = 'SELECT * FROM {}'.format(table_label)
        current_db_table = self.run_sql_query(self.engine, [query])
        current_db_table_cols = np.sort(current_db_table.columns)
        update_table_columns = np.sort(update_table.columns)

        #assert current_db_table_cols == update_table_columns

        cols_to_compare = current_db_table_cols[current_db_table_cols != pk]
        updated_row_pkids = {}
        # This is very terribly done, but I could not find a succinct way to compare dfs if they didnt 
        # have the same indices.
        # For each Primary Key Id in the new update_table
        for pk_id in update_table[pk]:
            # If the id is not in the current table, add it to a dictionary of pk_ids that are being updated.
            if pk_id not in current_db_table[pk].tolist():
                updated_row_pkids[pk_id] = table_label
            # if the id is in the current table, check to see if the values in the new update table match the 
            # current table. If they dont add it to a dictionary of pk_ids that are being updated.
            elif pk_id in current_db_table[pk].tolist():
                current_row = current_db_table.loc[current_db_table[pk] == pk_id].reindex(columns=current_db_table_cols)
                update_row = update_table.loc[update_table[pk] == pk_id].reindex(columns=current_db_table_cols)
                if not np.array_equal(current_row.values, update_row.values):
                    updated_row_pkids[pk_id] = table_label
        return updated_row_pkids

    def get_resource_ids(self, updated_row_pkids):
        '''
        Work back from primary key id to get back to the resource_id. should be able to use 
        get_component_dependencies.
        '''
        cdg = self.sg.se.get_digraph_by_edge_type('requiresComponent')
        for pkid, table_label in updated_row_pkids.items():
            # Get component digraph
            
            paths = nx.all_simple_paths(self.mm_graph, source=table_label, target='Resource')            
        return []

    def update_date_modified(self, resource_ids):
        return

    def update_db_tables(self, input_table:pd.DataFrame, validate: bool = False, full_validation: bool = False) -> List[str]: 
        """ Given an input table dataframe, that may be a denormalized view of data across multiple DB tables, 
        generate a set of normalized dataframes and update the corresponding DB tables 
        by *upserting* them with data in the dataframe.
         

        Args:
            input_table: a dataframe containing DB updates
            validate: TODO: if True, validate each table update matches the metadata model schema for that table 
            only update a table if the validation passes for that table; if False update tables 
            and do not validate (e.g. validation can be done later on the entire table row set)
            full_validation: TODO: if True, validate each table update matches the metadata model schema for 
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
        updated_row_pkids = {}
        for table_label in target_tables:

            pk = self.rdb.tables[table_label]['primary_key']
            fks = self.rdb.tables[table_label]['foreign_keys']
            # parse fk names if still linked with fk_attr, this will make sure
            # the table is updated properly and the column names match.
            fks = [fk.split('.')[1] if '.' in fk else fk for fk in fks]

            # get table attributes based on schema
            table_attributes = list(self.rdb.tables[table_label]['attributes'].keys()) 
            table_attributes += [pk] 
            if fks: 
                table_attributes += fks
            
            # get the relevant set of attribute columns from update data frame if they exist
            update_table = input_table[input_table.columns & table_attributes]
        
            # the update table might not have all columns in the table schema
            # fill the missing columns w/ nulls, if necessary
            for missing_column in set(table_attributes).difference(update_table.columns):
                 
                # if the not found column in schema is a foreign key 
                # ignore; there should be a corresponding column in the manifest w/o foreign key table prefix 
                if missing_column in fks:
                    continue
                
                # by default, fill in missing columns w/ None
                # todo change way this is done to comply with warning
                col_present = self.check_db_columns(table_label, missing_column)
                if not col_present:
                    update_table.loc[:, missing_column] = None
            
            # if resource is being explicitily update, update the UNIX times.
            '''
            if 'dateModified' in set(table_attributes):
                update_table['dateModified'] = int(time.time())
            '''

            # normalize update table
            update_table = df_utils.normalize_table(update_table, self.rdb.tables[table_label]['primary_key'])

            #updated_row_pkids.update(self.get_updated_rows(table_label, update_table, pk))
            if validate:
                pass # TODO: call json schema validator on update_table; do not generate table update statement if table doesn't match the schema
            if full_validation:
                pass # TODO: change code logic to validate tableas and do not generate any table update statements if even a single table fails validation
            # get sql table replace query
            updated_tables.append(self.update_table_sa(table_label, update_table))

        resource_ids = self.get_resource_ids(updated_row_pkids)
        #self.update_date_modified(resource_ids)
        
        return updated_tables

    
    def replace_table_sa(self, table_label: str, replace_table: pd.DataFrame) -> str:

        """ Given a normalized table data frame, update corresponding DB table indicated by table_label.
        This is a *replace* operation: the existing table is dropped and replaced by the data in the update_table.
        This operation will not work if the table contains a key referenced in another table.

        Args:
            table_label: target table to update
            replace_table: a data frame containing table updates

        Returns:
            If table update completes, return table_label
        """
        
        replace_table.to_sql(table_label, self.engine, if_exists = 'replace', index = False)

        return table_label

    def run_sql_query(self, sql_model, query):
        df = pd.read_sql(query[0], self.connection)
        df = df.loc[:,~df.columns.duplicated()]
        return df

    def viz_sa_schema(self, output_path: str) -> str:
        """ From sqlalchemy recipes:
        https://github.com/sqlalchemy/sqlalchemy/wiki/SchemaDisplay

        Visualize the schema network.

        Args:
            output_path: path to output schema png image

        Returns:
            output_path
        """

        # bind sqlalchemy metadata to db engine
        self.metadata.reflect(bind = self.engine)

        graph = create_schema_graph(metadata = self.metadata,
        show_datatypes = False, # The image would get nasty big if we'd show the datatypes
        show_indexes = False, # ditto for indexes
        rankdir = 'LR', # From left to right (instead of top to bottom)
        concentrate = False # Don't try to join the relation lines together
        )
        
        graph.write_png(output_path) # write out the file

        return output_path
