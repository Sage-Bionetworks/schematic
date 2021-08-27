import os
import logging

import pytest
import pandas as pd

from schematic.db.rdb import RDB
from schematic.db.sql import SQL

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture
def sql_model(helpers):

    rdb_model = RDB(
        path_to_json_ld=helpers.get_data_path("nfti.rdb.model.jsonld"),
        requires_component_relationship = "requiresComponent"
    )


    username = "root"
    password = "md_Sage_pw_86"
    host = "localhost"


    connection = str("mysql://{0}:{1}@{2}/".format(username, password, host)) + rdb_model.schema_name
    
    sql_model = SQL(
        rdb_model,
        connection,
    )

    yield sql_model


class TestSQL:

      
    def test_create_db_tables(self, sql_model):

        output = sql_model.create_db_tables()

        assert output is None
    
       
    def test_update_db_tables(self, sql_model):

        # set to absolute path to test manifest e.g.
        # for Mac OS X
        #"/Users/<user>/<path_to_schematic>/schematic/tests/data/mock_manifests/example.rdb.manifest.csv"
        # TODO: change to work with relative path to tests folder (see other test modules)
        # To generate manifest from example schema, run (make sure config uses RDB model above)
        # schematic manifest -v INFO --config ./config.yml get --data_type PatientBiospecimenComponent --oauth --sheet_url

        manifest_path = ""
        input_table = pd.read_csv(manifest_path)

        output = sql_model.update_db_tables(input_table)

        assert output is not None

    
    def test_viz_sa_schema(self, sql_model):

        # set to absolute path to test manifest e.g.
        # for Mac OS X along the lines of
        #"/Users/<user>/<path_to_schematic>/schematic/tests/data/" + sql_model.schema_name + ".rdb.model.png"
        # TODO: change to work with relative path to tests folder (see other test modules)

        output_path = "" 
        output = sql_model.viz_sa_schema(output_path)

        assert output == output_path

