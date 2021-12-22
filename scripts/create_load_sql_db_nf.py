import argparse
from datetime import datetime, timezone, tzinfo, timedelta # remove later

import os
from os import walk
import logging

from pathlib import Path
import pytest
import pandas as pd
import yaml


from schematic.utils.io_utils import load_json
from schematic.db.rdb import RDB
from schematic.db.sql import SQL

from schematic.utils.sql_utils import sql_helpers

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'tests', 'data')


class sql_create_load():
    def __init__(self,
        data_dir,
        rdb_jsonld_filename,
        sql_config,
            ) -> None:
        '''
        '''

        self.path_to_json_ld = sql_helpers.get_data_path(data_dir, rdb_jsonld_filename)

        self.rdb_model = RDB(
            path_to_json_ld=self.path_to_json_ld,
            requires_component_relationship = "requiresComponent"
        )

        self.json_data_model = load_json(self.path_to_json_ld)

        path_to_sql_config = os.path.join(Path(os.getcwd()).parent, 'schematic_package_docs', 'sql_config.yml')
        with open(path_to_sql_config) as f:
            var = yaml.load(f)

        connection = str("mysql://{0}:{1}@{2}/".format(var['username'],
            var['password'], var['host'])) + self.rdb_model.schema_name
        
        self.sql_model = SQL(
            self.rdb_model,
            connection,
        )
        
    def create_db_tables(self):

        output = self.sql_model.create_db_tables()

        assert output is None

    def update_db_tables(self, path_to_manifests):
        manifest_root_folder = str(Path(path_to_manifests).resolve())
        manifest_folders = [folder for folder in os.listdir(manifest_root_folder) 
                            if os.path.isdir(os.path.join(
                                manifest_root_folder, folder))
                            ]
        file_update_order = self.rdb_model.get_tables_update_order()
        for file in file_update_order:
            for i, f in enumerate(manifest_folders):
                manifest_file_name = f + '_nfti_' + file + '.rdb.manifest.csv'
                manifest_path = os.path.join(manifest_root_folder, f, manifest_file_name)
                if os.path.exists(manifest_path):
                    try:
                        input_table = pd.read_csv(os.path.join(manifest_path))

                        output = self.sql_model.update_db_tables(input_table)

                        assert output is not None
                    except:
                        breakpoint()
        
    def viz_sa_schema(self, output_path):

        # set to absolute path to test manifest e.g.
        # for Mac OS X along the lines of
        #"/Users/<user>/<path_to_schematic>/schematic/tests/data/" + sql_model.schema_name + ".rdb.model.png"
        # TODO: change to work with relative path to tests folder (see other test modules)
        output_path = str(Path(output_path).resolve())
        output = self.sql_model.viz_sa_schema(output_path)

        assert output == output_path

class parse_variables():
    def __init__(self,
        ) -> None:
        self.args = self._parse_args()
        self.helpers = helpers()
        self.var = sql_helpers.parse_config(self.args.path_to_configs, 'sql_query_config.yml')
        self.sql_query = sql_query(self.args.data_dir, self.args.rdb_jsonld_filename, self.args.sql_config)
        return
    def _parse_args(self):
        '''
        TODO: change default paths to be more general when pushing to
        rdb branch
        '''
        parser = argparse.ArgumentParser(description = 'Run sql queries and optionall push to synapse')
        parser.add_argument('-path_to_configs', default='', help='Path to folder containing config files')
        parser.add_argument('-create_db_tables', default=False, action = 'store_true')
        parser.add_argument('-update_db_tables', default=False, action = 'store_true')
        parser.add_argument('-viz_sa_schema', default=False, action = 'store_true')
        parser.add_argument('-output_path', '--o', default="schematic/tests/data/")
        parser.add_argument('-path_to_data')
        parser.add_argument('-rdb_jsonld_filename')
        args = parser.parse_args()
        return args

    def set_arguments(self):
        '''
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

        return arguments

if __name__ == '__main__':
    # TODO: Move from using argparse to CLI
    

    # Perform actions based on user input.
    if args.create_db_tables:
        sql_create_load(args.data_dir, args.rdb_jsonld_filename, args.sql_config).create_db_tables()

    if args.update_db_tables:
        if not args.path_to_data:
            print('No path provided to manifests')
        else:
            # add in an additional check to ensure the path is pointing to subfolders
            # containing csv files.
            sql_create_load(args.rdb_jsonld_filename, args.sql_config).update_db_tables(args.path_to_data)

    if args.viz_sa_schema:
        sql_create_load(args.rdb_jsonld_filename, args.sql_config).viz_sa_schema(args.output_path)

