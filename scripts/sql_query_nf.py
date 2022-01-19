'''
This is a custom version of the sql query script used to instantiate
the NF Tools Registry database.
'''
import argparse
import csv
import hashlib
import logging
import math
import numpy as np
import os
from os import walk
from pathlib import Path
import pandas as pd
import sys
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Text,
)

from schematic.db.rdb import RDB
from schematic.db.sql import SQL

from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import convert_string_cols_to_json
from schematic.utils.io_utils import load_json
from schematic.utils.sql_utils import sql_helpers
from schematic import CONFIG


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# any date columns that contain UNIX time.
DATE_COLS = ['publicationDateUnix', 'dateAdded', 'dateModified']
# Any column names with the name ID that should not be used when creating the 
# md5 hash code (ie any id columns that might be added to a row at a future date).
ID_COLS_TO_IGNORE = ['investigatorSynapseId']

# Column names to be updated due to differences in camelcase used by differnet
# programs. The need for this will be fixed in a future release.
# TODO:sql model has incorrect camel case for 'modelofManifestation'
# need to change this in the future.
COL_NAME_UPDATES = {'modelofManifestation': 'modelOfManifestation',
                    'animalModelofManifestation': 'animalModelOfManifestation'}

class sql_query():

    def __init__(self, 
            data_dir, 
            rdb_jsonld_filename, 
            path_to_configs
            ) -> None:
        ''' Initialize sql query class.
        '''
        self.path_to_json_ld = sql_helpers.get_data_path(data_dir, rdb_jsonld_filename)

        rdb_model = RDB(
            path_to_json_ld=self.path_to_json_ld,
            requires_component_relationship = "requiresComponent"
        )

        self.json_data_model = load_json(self.path_to_json_ld)
        
        var = sql_helpers.parse_config(path_to_configs, 'sql_config.yml')
        connection = str("mysql://{0}:{1}@{2}/".format(var['username'],
            var['password'], var['host'])) + rdb_model.schema_name
        
        self.sql_model = SQL(rdb_model, connection)
        return

    def _get_cols_to_convert(self, col_name_updates):
        '''Get list of columns, where the values need to be connverted to a JSON 
        string that can be read parsed by the Synapse portal.
        Also camelCase naming is not consistent from json schema and rdb. 
        For problematic column names, convert them so they will be properly 
        rendered in portal.

        TODO: Go into RDB code to figure out why naming is not being consistently 
        set. This function can then be depreciated.

        Args:
            col_name_updates (dict):
                Key: Current column name
                Value: New column name
        Returns:
            cols_to_convert (list):
                list of columns whose contents need to be converted to JSON Strings.
        '''
        # Identify list columns.
        cols_to_convert = []
        for row in self.json_data_model['@graph']:
            try:
                validation_rules = row['sms:validationRules']
            except:
                validation_rules = []
            if 'list' in validation_rules:
                cols_to_convert.append(row['rdfs:label'])

        # Update column names (if applicable)
        for curr_col_name, new_col_name in col_name_updates.items():
            if curr_col_name in cols_to_convert:
                cols_to_convert.remove(curr_col_name)
                cols_to_convert.append(new_col_name)
        return cols_to_convert

    def _convert_dates(self, df, date_cols):
        '''Convert dates to UNIX time * 1000
        Args:
            df (pd.DataFrame): df containing the outputs of a sql query
            date_cols (list): list of column names that contain dates that 
                need to be converted.
        Returns:
            df (pd.DataFrame): Updated df where columns containing UNIX 
                time have been multiplied by 1000.
        '''
        for col in df.columns:
            for dc in date_cols:
                if col == dc:
                    df[col] = df[col].apply(lambda x: int(x) * 1000)
        return df

    def _add_md5_hash(self, df, id_cols_to_ignore):
        '''Add a checksum column to keep track of rows, based on the combination
        of id columns. This will allow table updates on Synapse so changes 
        within rows can be identified.
        
        Args:
            df (pd.DataFrame): df containing the outputs of a sql query
            id_cols_to_ignore (list): List of columns that have `Id` at the end
                of their name that we do not want to be part of our md5 hash.
                These should be columns that are PKs or FKs
        Returns:
            df (pd.DataFrame): same as input df but contains an additional
                column of md5 checksums calculated per row, based on a join
                of all id columns.
        '''
        # Get all Id columns
        m = df.columns.str.endswith("Id")
        # Exclude columns that should not be used to calucluate md5 hash.
        for id_to_ignore in id_cols_to_ignore:
            if id_to_ignore in df.columns:
                update_m_idx = df.columns.get_loc(id_to_ignore)
                m[update_m_idx] = False
        # Get all columns to encode and sort (so the calc will be consistent
        # for updates)
        cols_to_encode = [x for x in m * df.columns if x]
        cols_to_encode.sort()
        # Join all column names into a single string
        joined_ids = joined_ids = df[cols_to_encode].apply(lambda x: ''.join(x.dropna()), axis=1)
        # Calculate md5 has from joined ids. Add as a new column to df.
        df['md5_id'] = [hashlib.md5(j.encode('utf-8')).hexdigest() for j in joined_ids]
        return df

    def _parse_synapse_column_types(self, folder:str, file_name: str) -> Dict:
        '''Use csv defining synapse column types to create a dictionary defining
        column types and sizes. Will be used when creating a synapse schema.
        
        Args:
            folder (str): path to where the csv file is stored.
            file_name (str): name of the column_type file

        Returns: 
            column_type_dictionary (dict): A nested dictionary, 
                Primary Keys are the column name.
                Secondary Keys are 'column_type', 'maximum_size' and 'maximum_length' (if applicable).
                Values are taken from the input values in the spreadsheet.

        '''
        path = os.path.join(str(Path(folder).resolve()), file_name)
        column_type_dict = {}
        with open(path, mode='r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                column_type_dict[row['column_name']] = {}
                column_type_dict[row['column_name']]['column_type'] = row['column_type']
                if row['maximum_size']:
                    column_type_dict[row['column_name']]['maximum_size'] = int(row['maximum_size'])
                else:
                    column_type_dict[row['column_name']]['maximum_size'] = None
                if row['maximum_list_length']:
                    column_type_dict[row['column_name']]['maximum_list_length'] = int(row['maximum_list_length'])
                else:
                    column_type_dict[row['column_name']]['maximum_list_length'] = None
        return column_type_dict

    def load_queries(self, data_dir: str,  query_csv: str) -> pd.DataFrame:
        '''Load csv containing query info into pandas dataframe.

        Args:
            data_dir (str): path to where the query csv is stored.
            query_csv(str): name of query file

        Returns: pandas df that mimics the query csv.
        '''
        return pd.read_csv(os.path.join(str(Path(data_dir).resolve()), 
                    query_csv))

    def _gather_query_data(self, queries_df: pd.DataFrame, name_of_query: str) -> [List[list], bool]:
        '''
        Gather all data for the query(s) to be performed and move into a list of lists, either for a single query or all as defined by the user.
        Args:
            queries_df (df): pandas df that mimics the query csv. 
            name_of_query (str): User input name of query (corresponds to its Synapse Table Name)
                            or 'all'. Running all will run all queries.

        Returns: 
            list of lists containing the following for each query:
                query_strs (str): formatted string used to perform sql query.
                table_names (str): name of the synapse table
                existing_table_ids (str): name of the synapse table id for an existing query table.
                define_schema (bool): indicates whether the Synapse table schema should be specified (True) or inferred (False).
            bool:
                Defines whether to create a column type dictionary or not. Determined from user inputs.
        '''
        if arguments['name_of_query'].lower() != 'all':
            self.query_index = queries_df.index[queries_df['Table Name'] == arguments['name_of_query']].tolist()
            query_strs = queries_df['MySql Query'][self.query_index].replace('\n', " ").tolist()[0]
            table_names = queries_df['Table Name'][self.query_index].tolist()[0]
            existing_table_ids = queries_df[arguments['table_id_type']][self.query_index].tolist()[0]
            if type(existing_table_ids) != str and math.isnan(existing_table_ids):
                existing_table_ids = None
            define_schema = queries_df['Define Column Schema'][self.query_index].tolist()[0]
            #set all nan entries to false
            if math.isnan(define_schema):
                define_schema = False
            return [[query_strs, table_names, existing_table_ids, define_schema]], define_schema
        elif arguments['name_of_query'].lower() == 'all':
            query_strs = [q.replace('\n', " ") for q in queries_df['MySql Query']]
            table_names = queries_df['Table Name'].tolist()
            existing_table_ids = queries_df[arguments['table_id_type']].tolist()
            existing_table_ids = [None if type(v) != str and math.isnan(v) else v for v in existing_table_ids]
            define_schema = queries_df['Define Column Schema'].tolist()        
            #set all nan entries to false
            define_schema = [False if math.isnan(v) else v for v in define_schema]
            if sum(define_schema)> 1:
                need_column_types=True
            else:
                need_column_types=False
            queries = [list(a) 
                    for a in zip(query_strs, table_names, 
                        existing_table_ids, define_schema)]
        return queries, need_column_types

    def run_sql_queries(self, arguments):
        '''Loads sql queries and cleans up the outputs for loading into synapse.
        Args:
            arguments (dict): dictionary containing arguments and their values.
                a combination of user entered values, from the command line,
                config file and hard coded entries.
        Returns:
            None
        '''

        # Load config file so SynapseStorage can access attributes.
        try:
            logger.debug(f"Loading config file contents in '{os.path.join(arguments['path_to_configs'], 'config.yml')}'")
            load_config = CONFIG.load_config(str(Path(os.path.join(arguments['path_to_configs'], 'config.yml')).resolve()))
        except ValueError as e:
            logger.error("'--config' not provided or environment variable not set.")
            logger.exception(e)
            sys.exit(1)
        
        if arguments['save_local']:
            # Make output directory
            output_dir = sql_helpers.make_output_dir()

        #Load the sql query and query name
        queries_df = self.load_queries(
                    arguments['rdb_data_dir'], 
                    arguments['query_csv'])

        # Get list of columns that need to be converted from str to json
        cols_to_convert = self._get_cols_to_convert(arguments['col_name_updates'])

        # Put relevant query data into a list.
        queries, need_column_types = self._gather_query_data(queries_df, arguments['name_of_query'])
        
        # Get column types for synapse if necessary for query:
        column_type_dict = {}
        if need_column_types:
            column_type_dict = self._parse_synapse_column_types(arguments['rdb_data_dir'], arguments['column_types_csv'])

        # For each query make output table.
        for query in queries:
            df = self.sql_model.run_sql_query(self.sql_model, [query[0]])
            df = convert_string_cols_to_json(df, cols_to_convert)

            # Put id columns up front.
            m = df.columns.str.endswith("Id")
            cols = df.columns[m].append(df.columns[~m])
            df = df[cols]
            
            # Update UNIX time columns so they will work in synapse.
            df = self._convert_dates(df, arguments['date_cols'])

            # Add checksum
            df = self._add_md5_hash(df, arguments['id_cols_to_ignore'])

            # Convert all string Nans to actual nans
            df.replace('NaN', np.nan, inplace=True)
            df.where(pd.notnull(df), None)
            # Either save df locally or upload to synapse (or both).
            if arguments['save_local']:
                output_file_path = sql_helpers.make_output_path(output_dir, query[1])
                df.to_csv(output_file_path)
            if arguments['save_to_synapse']:
                table_name, existing_table_id, specify_schema = query[1:]
                syn_store = SynapseStorage()
                # When trouleshooting reintroduce the breakpoint so that
                # you can catch any errors, or else they will proceed silently.
                make_synapse_table = syn_store.make_synapse_table(df, arguments['synapse_project_folder'], existing_table_id, table_name, column_type_dict, specify_schema)
        return

class parse_variables():
    def __init__(self,
        ) -> None:
        self.args = self._parse_args()
        self.var = sql_helpers.parse_config(self.args.path_to_configs, 'sql_query_config.yml')
        return

    def _parse_args(self):
        ''' Pull in command line arguments.
        Returns: args input by the user
        '''
        parser = argparse.ArgumentParser(description = 'Run sql queries and optionall push to synapse')
        
        # Paths to files and filenames
        parser.add_argument('-data_dir', help='Path to folder where rdb.model.jsonld \
                is located')
        parser.add_argument('-rdb_data_dir', help='Path to folder should contain folders \
         of completed manifests, column_types.csv and joins_for_synapse.csv')
        parser.add_argument('-column_types_csv', help='Name of the file that contains the column type specification.')
        parser.add_argument('-rdb_jsonld_filename', help='Name of the rdb.model.jsonld file')
        parser.add_argument('-query_csv')
        parser.add_argument('-path_to_configs', default='schematic', help='Path to folder containing config files')
        
        # Synapse Arguments
        parser.add_argument('-save_to_synapse', help='Update tables in synapse', default=False, action='store_false')
        parser.add_argument('-table_id_type', help='Is the folder type being saved to on synapse, defined in joins csv column name')
        parser.add_argument('-synapse_project_folder')
        parser.add_argument('-save_local', default=True, action='store_true')
        
        # SQL Arguments
        parser.add_argument('-name_of_query', help='Table Name of query to run or \'all\' to run all queries')
        args = parser.parse_args()
        return args

    def query_name_valid(self, arguments):
        '''Check if the name of the query provided matches those available
        Or is 'all', indicating that all available queries should be run.
        Args:
            data_dir (str): Path to folder where rdb.model.jsonld is located.
            query_csv (str): name of the query csv file.
            name_of_query (str): name of query to be run
        Returns:
            bool: indicates whether the query is valid.
        '''
        self.sql_query = sql_query(arguments['data_dir'], arguments['rdb_jsonld_filename'], 
            arguments['path_to_configs'])
        queries = self.sql_query.load_queries(arguments['rdb_data_dir'], arguments['query_csv'])
        if arguments['name_of_query'] not in queries['Table Name'].values and arguments['name_of_query'].lower() != 'all':
            logger.error(
                f"The query you are trying to run {arguments['name_of_query']} is either not defined " \
                f"or the name is typed incorrectly. Please check and try again. ")
            return True
        else:
            return False
        return

    def check_arguments(self, arguments):
        ''' Check if provided arguments are valid. Can be expanded in future.
        Args:
            arguments (dict): dictionary containing arguments and their values.
                a combination of user entered values, from the command line,
                config file and hard coded entries.
        Returns:
            arg_errors (list): list of bool values indiciating whether
                there were errors in the arguments provided.
        '''
        arg_errors = []
        arg_errors.append(
            self.query_name_valid(
                arguments,
                )
            )
        return arg_errors

    def set_arguments(self):
        ''' Gather all arguments provided by the user fom the config file and 
        args parser, along with hard coded values added at the top of this script.
        
        Args:
            Self : self.args: parsed command line arguments
                   self.vars: parsed config file
            Global hard coded values.
        Returns:
            arguments (dict): dictionary containing arguments and their values.
                a combination of user entered values, from the command line,
                config file and hard coded entries.
        '''
        arguments = {}
        # Load hardcoded arguments.
        arguments['date_cols'] = DATE_COLS
        arguments['id_cols_to_ignore'] = ID_COLS_TO_IGNORE
        arguments['col_name_updates'] = COL_NAME_UPDATES
        
        # Add config file to arguments
        arguments.update(self.var)

        # If a command line arg is supplied, overwrite the
        # config version.
        parsed_args = vars(self.args)
        for key, value in parsed_args.items():
            if value:
                arguments[key] = value

        logger.debug(f"Running a quick check on provided arguments")
        arg_errors = self.check_arguments(arguments)
        if sum(arg_errors) > 0:
            sys.exit(1)
        return arguments

if __name__ == '__main__':
    arguments = parse_variables().set_arguments()
    sql_query(arguments['data_dir'], arguments['rdb_jsonld_filename'], arguments['path_to_configs']).run_sql_queries(arguments)
    