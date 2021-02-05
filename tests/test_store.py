import os
import logging
import pytest

import pandas as pd
from dotenv import load_dotenv

from schematic.store import BaseStorage
from schematic.store import SynapseStorage


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


load_dotenv()


@pytest.fixture
def synapse_store():
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()
    return synapse_store


class TestBaseStorage:

    def test_init(self):

        with pytest.raises(NotImplementedError):
            BaseStorage()


class TestSynapseStorage:

    def test_init(self, synapse_store):
        assert synapse_store.storageFileview == "syn23643253"
        assert isinstance(synapse_store.storageFileviewTable, pd.DataFrame)


    def test_getFileAnnotations(self, synapse_store):
        expected_dict = {
            "author": "bruno, milen, sujay",
            "impact": "42.9, 13.2",
            "confidence": "high",
            "YearofBirth": "1980",
            "entityId": "syn24226530",
        }
        actual_dict = synapse_store.getFileAnnotations("syn24226530")

        # For simplicity, just checking if eTag is present since
        # it changes anytime the files on Synapse change
        assert "eTag" in actual_dict
        del actual_dict["eTag"]

        assert expected_dict == actual_dict


    def test_getDatasetAnnotations(self, synapse_store):
        expected_df = pd.DataFrame.from_records([
            {
                "author": "bruno, milen, sujay",
                "impact": "42.9, 13.2",
                "confidence": "high",
                "YearofBirth": "1980",
                "entityId": "syn24226530",
            },{
                "confidence": "low",
                "date": "2020-02-01",
                "entityId": "syn24226531",
            },{
                "entityId": "syn24226532",
            }
        ])
        actual_df = synapse_store.getDatasetAnnotations("syn24226514")

        # For simplicity, just checking if eTag is present since
        # it changes anytime the files on Synapse change
        assert "eTag" in actual_df
        actual_df.drop(columns="eTag", inplace=True)

        pd.testing.assert_frame_equal(expected_df, actual_df)
