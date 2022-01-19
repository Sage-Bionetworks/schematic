import argparse
from datetime import datetime, timezone, tzinfo, timedelta # remove later

import os
from os import walk
import logging

from pathlib import Path
import pytest
import pandas as pd
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Text,
    BinaryIO,
)
import yaml


from schematic.utils.io_utils import load_json
from schematic.db.rdb import RDB
from schematic.db.sql import SQL

from schematic.utils.sql_utils import sql_helpers

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

#DATA_DIR = os.path.join(os.getcwd(), 'tests', 'data')


class sql_create_load():
    def __init__(self,
        data_dir: str,
        rdb_jsonld_filename: str,
        path_to_configs: str,
            ) -> None:
        '''
        Args: 
            data_dir (str): path to where the jsonld files are stored.
            rdb_jsonld_filename (str): name of the jsonld file used to create the RDB.
            path_to_configs (str): relative or absolute path to config files.
        Returns: None
        '''

        self.path_to_json_ld = sql_helpers.get_data_path(data_dir, rdb_jsonld_filename)
        self.rdb_model = RDB(
            path_to_json_ld=self.path_to_json_ld,
            requires_component_relationship = "requiresComponent"
        )

        self.json_data_model = load_json(self.path_to_json_ld)

        path_to_sql_config = str(Path(os.path.join(path_to_configs, 'sql_config.yml')).resolve())
        with open(path_to_sql_config) as f:
            var = yaml.load(f)

        connection = str("mysql://{0}:{1}@{2}/".format(var['username'],
            var['password'], var['host'])) + self.rdb_model.schema_name
        
        self.sql_model = SQL(
            self.rdb_model,
            connection,
        )
        
    def create_db_tables(self):
        ''' Create rdb tables using the data model provided in the JSONLD. Database
        is created in a local instantiation.
        Args: Self
        Returns: None
        '''
        output = self.sql_model.create_db_tables()

        assert output is None

    def update_db_tables(self, path_to_manifests: str) -> None:
        ''' Takes user provided manifests and loads them into the database created 
        in create_db_tables.
        Args:
            Self,
            path_to_manifests (str):
        Returns:
        '''
        # Get all the folders containing manifests.
        manifest_root_folder = str(Path(path_to_manifests).resolve())
        manifest_folders = [folder for folder in os.listdir(manifest_root_folder) 
                            if os.path.isdir(os.path.join(
                                manifest_root_folder, folder))
                            ]
        # Get the order to load the files.
        file_update_order = self.rdb_model.get_tables_update_order()
        # Load data by file type, by folder.
        for file in file_update_order:
            for i, f in enumerate(manifest_folders):
                # Construct manifest filename and path
                manifest_file_name = f + '_nfti_' + file + '.rdb.manifest.csv'
                manifest_path = os.path.join(manifest_root_folder, f, manifest_file_name)
                if os.path.exists(manifest_path):
                    try:
                        # Create update df
                        input_table = pd.read_csv(os.path.join(manifest_path))
                        # Update table with data in the database.
                        output = self.sql_model.update_db_tables(input_table)

                        assert output is not None
                    except:
                        breakpoint()
        
    def create_schema_viz(self, output_path: str) -> None:
        ''' Generate a ERD diagram depicting the sql model.
        Args: self: containing the sql model.
             output_path (str): relative or aboslute path where figure should be stored.
        Returns: ERD Diagram saved to output path as a png.
        '''
        output_path = str(Path(output_path).resolve())
        output = self.sql_model.viz_sa_schema(output_path)

        assert output == output_path

class parse_variables():
    def __init__(self,
        ) -> None:
        ''' Loads all user defined parameters from the config file and command line
        arguments.
        '''
        self.args = self._parse_args()
        self.var = sql_helpers.parse_config(self.args.path_to_configs, 'sql_query_config.yml')
        return
    
    def _parse_args(self) -> BinaryIO:
        ''' Pull in command line arguments.
        Returns: args input by the user
        '''
        # Paths to files and filenames
        parser = argparse.ArgumentParser(description = 'Run sql queries and optionall push to synapse')
        parser.add_argument('-data_dir', help='Path to folder where rdb.model.jsonld \
                is located')
        parser.add_argument('-path_to_configs', default='', help='Path to folder containing config files')
        parser.add_argument('-output_path', '--o', default="schematic/tests/data/")
        parser.add_argument('-rdb_jsonld_filename')
        
        # Select which functions to run.
        parser.add_argument('-create_db_tables', default=False, action = 'store_true')
        parser.add_argument('-update_db_tables', default=False, action = 'store_true')
        parser.add_argument('-create_schema_viz', default=False, action = 'store_true')
        args = parser.parse_args()
        return args

    def set_arguments(self) -> Dict:
        '''Gather all arguments provided by the user fom the config file and 
        args parser. Command line arguments will supercede config file args.
        
        Args:
            Self : self.args: parsed command line arguments
                   self.vars: parsed config file
        Returns:
            arguments (dict): dictionary containing arguments and their values.
                a combination of user entered values, from the command line,
                config file and hard coded entries.
        '''
        arguments = {}
        # Add config file to variables
        arguments.update(self.var)

        # If a command line arg is supplied, overwrite the
        # config version.
        parsed_args = vars(self.args)

        for key, value in parsed_args.items():
            if value:
                arguments[key] = value
            if key == 'create_db_tables' or key == 'update_db_tables' or key == 'create_schema_viz':
                arguments[key] = value
        return arguments

if __name__ == '__main__':
    #Note: -path_to_configs needs to be supplied by user.
    arguments = parse_variables().set_arguments()
    # Perform actions based on user input.
    if arguments['create_db_tables']:
        sql_create_load(arguments['data_dir'], arguments['rdb_jsonld_filename'], arguments['path_to_configs']).create_db_tables()

    if arguments['update_db_tables']:
        if not arguments['data_dir']:
            logger.error('No path provided to manifests')
            sys.exit(1)
        else:
            # add in an additional check to ensure the path is pointing to subfolders
            # containing csv files.
            sql_create_load(arguments['data_dir'], arguments['rdb_jsonld_filename'], arguments['path_to_configs']).update_db_tables(arguments['rdb_data_dir'])

    if arguments['create_schema_viz']:
        sql_create_load(arguments['data_dir'], arguments['rdb_jsonld_filename'], arguments['path_to_configs']).viz_sa_schema(arguments['output_path'])

