import argparse
import os
from os import walk
from pathlib import Path


import logging

import pandas as pd

from schematic.db.rdb import RDB
from schematic.db.sql import SQL

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'tests', 'data')

# TODO:
# Format citation data in JSONLD format.

class sql_query():

    def __init__(self,
            ) -> None:
        
        self.sql_queries = [
        ('SELECT * FROM Resource', \
            'syn26344826'),
        ('SELECT * FROM `Development` \
            JOIN `Investigator` \
            USING(investigatorId)', \
            'syn26344827'),
        ('SELECT * FROM `Development` \
            JOIN `Publication`\
            USING(publicationId)',\
            'syn26344828'),
        ('SELECT * FROM `Development` \
            JOIN `Funder`\
            USING(funderId)',\
            'syn26344829'),
        ('SELECT * from `Resource` \
            INNER JOIN `VendorItem` \
            USING(resourceID) \
            INNER JOIN `Vendor` \
            USING(vendorId)',\
            'syn26344830'),
        ('SELECT * FROM `ResourceApplication`', \
            'syn26344831'),
        ('SELECT * from `Observation` \
            INNER JOIN `Development` \
            USING(resourceId) \
            INNER JOIN `Publication` \
            USING(publicationId)',\
            'syn26344832'),
        ('SELECT * FROM \
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
            'syn26344833'),
        ]

        rdb_model = RDB(
            path_to_json_ld=self._get_data_path("nf_research_tools.rdb.model.jsonld"),
            requires_component_relationship = "requiresComponent"
        )

        username = "root"
        password = "md_Sage_pw_86"
        host = "localhost"


        connection = str("mysql://{0}:{1}@{2}/".format(username, password, host)) + rdb_model.schema_name
        
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

    def run_sql_queries(self):
        # Make output directory
        output_dir = self._make_output_dir()
        # For each query make output table.
        for query in self.sql_queries:
            self.sql_model.execute_and_save_query(self.sql_model, query, output_dir)
        return 

if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser(description = 'Run sql queries and optionall push to synapse')
    parser.add_argument('query_path', help='path to csv file containing the sql queries')
    parser.add_argument('-to-synapse', help='Update tables in synapse')
    args = parser.parse_args()
    '''
    sql_query().run_sql_queries()
