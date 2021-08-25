import os
import logging

import pytest

from schematic.db.rdb import RDB

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture
def rdb_model(helpers):

    rdb_model = RDB(
        path_to_json_ld=helpers.get_data_path("example.rdb.model.jsonld"),
        requires_component_relationship = "requiresComponent"
    )

    yield rdb_model


class TestRDB:
    def test_generate_tables(self, rdb_model):

        output = rdb_model.generate_tables()

        assert type(output) is dict
