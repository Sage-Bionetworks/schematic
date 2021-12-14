'''
This is a custom version of the sql query script used to instantiate
the NF Tools Registry database.
'''


import argparse
import csv
import hashlib
import numpy as np
import os
from os import walk
from pathlib import Path


import logging

import pandas as pd
import yaml

from schematic.db.rdb import RDB
from schematic.db.sql import SQL

from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import convert_string_cols_to_json
from schematic.utils.io_utils import load_json
from schematic import CONFIG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'tests', 'data')

class sql_query():

    def __init__(self,
            ) -> None:
        '''
        Purpose: Initialize sql query class.

        Inputs:

        TODO: 
        '''
            
        self.path_to_json_ld = self._get_data_path("nf_research_tools.rdb.model.jsonld")

        rdb_model = RDB(
            path_to_json_ld=self.path_to_json_ld,
            requires_component_relationship = "requiresComponent"
        )

        self.json_data_model = load_json(self.path_to_json_ld)

        path_to_sql_config = os.path.join(Path(os.getcwd()).parent, 'schematic_package_docs', 'sql_config.yml')
        with open(path_to_sql_config) as f:
            var = yaml.load(f)

        connection = str("mysql://{0}:{1}@{2}/".format(var['username'],
            var['password'], var['host'])) + rdb_model.schema_name
        
        self.sql_model = SQL(
            rdb_model,
            connection,
        )
    def _get_data_path(self, path, *paths):
        return os.path.join(DATA_DIR, path, *paths)
    
    def _make_output_dir(self):
        parent_path = Path(os.getcwd()).parent
        output_dir = os.path.join(parent_path, 'schematic_sql_outputs')

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        return output_dir

    def _make_output_path(self, output_dir, filename):
        return os.path.join(output_dir, filename + '.csv')

    def _get_cols_to_convert(self):
        # camel case isnt consistent so need to update some column names so 
        # it will work for the portal.
        # In the future just use display names.
        col_name_updates = {'modelofManifestation': 'modelOfManifestation',
                        'animalModelofManifestation': 'animalModelOfManifestation'}

        cols_to_convert = []
        for row in self.json_data_model['@graph']:
            try:
                validation_rules = row['sms:validationRules']
            except:
                validation_rules = []
            if 'list' in validation_rules:
                cols_to_convert.append(row['rdfs:label'])

        #update_naming

        for curr_col_name, new_col_name in col_name_updates.items():
            if curr_col_name in cols_to_convert:
                cols_to_convert.remove(curr_col_name)
                cols_to_convert.append(new_col_name)
        return cols_to_convert

    def _convert_publication_date(self, df, date_cols):
        '''
        Normal UNIX time will not work on synapse. Need to multiply
        that time by 1000 to get accurate time on the Synapse schema.
        Modifying dates here rather than on the Database so the database
        will make sense for people who try to access it outside of Synapse.
        '''
        for col in df.columns:
            for dc in date_cols:
                if col == dc:
                    df[col] = df[col].apply(lambda x: int(x) * 1000)
        return df

    def _add_md5_hash(self, df):
        '''
        Add a checksum column to keep track of rows since we cannot
        rely on a single Id alone. This will allow table updates to 
        work properly on Synapse.
        '''
        # dont want to use investigatorSynapseId as part of the eventual Hex
        m = df.columns.str.endswith("Id")
        if 'investigatorSynapseId' in df.columns:
            update_m_idx = df.columns.get_loc('investigatorSynapseId')
            m[update_m_idx] = False
        cols_to_encode = [x for x in m * df.columns if x]
        cols_to_encode.sort()
        joined_ids = joined_ids = df[cols_to_encode].apply(lambda x: ''.join(x.dropna()), axis=1)
        df['md5_id'] = [hashlib.md5(j.encode('utf-8')).hexdigest() for j in joined_ids]
        return df

    def _parse_synapse_column_types(self, path):
        # if a path is provided to a file specifiying the column
        # types then populate the dictionarly.
        column_type_dict = {}
        if path:
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

    def run_sql_queries(self,save_local, save_to_synapse, config, path_to_column_types
        datasetId, sql_queries):
        # Try loading the config file.
        try:
            logger.debug(f"Loading config file contents in '{config}'")
            load_config = CONFIG.load_config(config)
        except ValueError as e:
            logger.error("'--config' not provided or environment variable not set.")
            logger.exception(e)
            sys.exit(1)

        if save_local:
            # Make output directory
            output_dir = self._make_output_dir()

        # load column type specifications used for creating synapse table schema
        column_type_dict = self._parse_synapse_column_types(path_to_column_types)

        # Get list of columns that need to be converted from str to json
        cols_to_convert = self._get_cols_to_convert()


        # For each query make output table.
        for query in sql_queries:
            # TODO:sql model has incorrect camel case for 'modelofManifestation'
            # need to change this in the future.
            df = self.sql_model.run_sql_query(self.sql_model, query)
            df = convert_string_cols_to_json(df, cols_to_convert)

            # Put id columns up front.
            m = df.columns.str.endswith("Id")
            cols = df.columns[m].append(df.columns[~m])
            df = df[cols]
            
            # Update UNIX time columns so they will work in synapse.
            df = self._convert_publication_date(df)

            # Add checksum
            df = self._add_md5_hash(df)

            # Convert all string Nans to actual nans
            df.replace('NaN', np.nan, inplace=True)
            df.where(pd.notnull(df), None)
            
            # Either save df locally or upload to synapse (or both).
            if save_local:
                output_file_path = self._make_output_path(output_dir, query[1])
                df.to_csv(output_file_path)
            if save_to_synapse:
                existing_table_id, table_name, specify_schema = query[1:]
                try:
                    syn_store = SynapseStorage()
                    # When trouleshooting reintroduce the breakpoint to that
                    # you can catch any errors, or else they will proceed silently.
                    # breakpoint()
                    make_synapse_table = syn_store.make_synapse_table(df, datasetId, 
                        existing_table_id, table_name, column_type_dict, specify_schema)
                except:
                    breakpoint()
        return 

if __name__ == '__main__':
    # TODO: now that i am pulling in the config file can try to just use that for the input
    # move to using the commands.
    parser = argparse.ArgumentParser(description = 'Run sql queries and optionall push to synapse')
    #parser.add_argument('query_path', help='path to csv file containing the sql queries')
    parser.add_argument('-path_to_column_types')
    parser.add_argument('-save_to_synapse', help='Update tables in synapse', default=False, action='store_true')
    parser.add_argument('-save_local', default=False, action='store_true')
    parser.add_argument('-config', default='../schematic_package_docs/config.yml')
    args = parser.parse_args()

    # Hard-coded additions:
    # ---------------------

    date_cols = ['publicationDateUnix', 'dateAdded', 'dateModified']
    id_cols_to_ignore = ['investigatorSynapseId']

    # Often used a Synapse Staging folder to test uploads and modifications
    # before pushing myself.
    
    datasetIds = ['syn26434836', 'syn26338068'] # Staging/Real
    datasetId = datasetIds[0] 

    # [ String Query, SynapseID of the table if it already exists - empty string if not,
    # String name of the table, If we are using a specified schema column types or not.]
    sql_queries = [
        ['SELECT * FROM Vendor', \
                    '', \
                    'Vendor', True],
        ]
        
    sql_query().run_sql_queries(args.save_local, args.save_to_synapse, args.config, 
        args.path_to_column_types, date_cols, datasetId, sql_queries)

    '''
    self.sql_queries = [
        ,
        ['SELECT * FROM Resource', \
                '', \
                'Resource'],
        ['SELECT * FROM `Development` \
        JOIN `Publication`\
        USING(publicationId)',\
        'syn26434909', 'Development_Publication'],
        ['SELECT * FROM Resource', \
            '', \
            'Resource'],
        ['SELECT * FROM `Development` \
            JOIN `Investigator` \
            USING(investigatorId)', \
            'syn26449830', 'Development_Investigator']
        ,
        ['SELECT * FROM `Development` \
            INNER JOIN `Funder`\
            USING(funderId)',\
            'syn26449846', 'Development_Funder']
        ],
        ['SELECT * from `Resource` \
            INNER JOIN `VendorItem` \
            USING(resourceID) \
            INNER JOIN `Vendor` \
            USING(vendorId)',\
            '', 'Resource_VendorItem_Vendor'],
        ['SELECT * FROM `ResourceApplication` \
            INNER JOIN `Usage` \
            USING(resourceId)\
            INNER JOIN `Publication` \
            USING(publicationId)',\
            '', 'ResourceApplication_Publication'],
        ['SELECT * from `Observation` \
            INNER JOIN `Development` \
            USING(resourceId) \
            INNER JOIN `Publication` \
            USING(publicationId)',\
            '', 'Observation_Publication'],
        ['SELECT * FROM \
            (SELECT R.resourceId, R.animalModelId, R.cellLineId, R.rrid, \
            R.resourceName,R.synonyms, R.resourceType, R.description, \
            R.mTARequired, R.usageRequirements, R.dateAdded, R.dateModified, \
            M.mutationId, M.mutationDetailsId\
            FROM `Resource` AS R\
            JOIN `Mutation` M\
            ON R.animalModelId = M.animalModelId \
            UNION ALL \
            SELECT R.resourceId, R.animalModelId, R.cellLineId, R.rrid, \
            R.resourceName,R.synonyms, R.resourceType, R.description, \
            R.mTARequired, R.usageRequirements, R.dateAdded, R.dateModified, \
            M.mutationId, M.mutationDetailsId\
            FROM `Resource` AS R\
            JOIN `Mutation` M\
            ON R.cellLineId = M.cellLineId) foo\
            JOIN MutationDetails md\
            ON md.mutationDetailsId = foo.mutationDetailsId',\
            '', 'Resource_Mutation_MutationDetails'],
        ['SELECT resourceId, rrid, resourceName, synonyms, \
            resourceType, description, mtaRequired, \
            usageRequirements, cellLineCategory, \
            cellLineDisease, modelOfManifestation, \
            backgroundStrain, backgroundSubstrain, \
            animalModelDisease, animalModelOfManifestation, \
            insertName, insertSpecies, vectorType,\
            targetAntigen, reactiveSpecies, hostOrganism \
            FROM (\
                SELECT resourceId, rrid, resourceName, \
                synonyms, resourceType, description, \
                mtaRequired, usageRequirements, cellLineCategory, \
                cellLineDisease, modelOfManifestation, \
                backgroundStrain, backgroundSubstrain, \
                animalModelDisease, animalModelOfManifestation, \
                insertName, insertSpecies, vectorType, \
                targetAntigen, reactiveSpecies, hostOrganism, \
                cellLineId, animalModelId, antibodyId \
                FROM Resource \
                LEFT JOIN cellLine USING(cellLineId) \
                LEFT JOIN animalModel USING(animalModelId) \
                LEFT JOIN geneticReagent USING(geneticReagentId) \
                LEFT JOIN antibody USING(antibodyId)) temp;',
                'syn26438037', 'Resource_CellLine_AnimalModel_GeneticReagent_Antibody'
                ]
        ]
        '''
