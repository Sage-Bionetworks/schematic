import os
import logging

import pytest

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
