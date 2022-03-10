import os
import logging
import pytest
from pathlib import Path

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.models.validate_manifest import validate_all
from schematic.models.metadata import MetadataModel
from schematic.store.synapse import SynapseStorage

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def synapse_store():
    access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
    if access_token:
        synapse_store = SynapseStorage(access_token=access_token)
    else:
        synapse_store = SynapseStorage()
    yield synapse_store


class TestManifestValidation:
    def test_valid_manifest(self,helpers,synapse_store):
        manifestPath = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
        rootNode = 'MockComponent'

        metadataModel= MetadataModel(
            inputMModelLocation =   helpers.get_data_path("example.model.jsonld"),
            inputMModelLocationType="local"
            )

        errors = MetadataModel.validateModelManifest(
            metadataModel,
            manifestPath=manifestPath,
            rootNode=rootNode)
        print(errors)
        assert errors == [[]]

        
        


    def test_invalid_manifest(self,helpers):
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = 'MockComponent'


        metadataModel= MetadataModel(
            inputMModelLocation =   helpers.get_data_path("example.model.jsonld"),
            inputMModelLocationType="local"
            )

        errors = MetadataModel.validateModelManifest(
            metadataModel, 
            manifestPath=manifestPath,
            rootNode=rootNode)

        assert len(errors) == 11

        assert GenerateError.generate_type_error(
            val_rule = 'num',
            row_num = 2,
            attribute_name = 'Check Num',
            invalid_entry = '6') in errors
        assert GenerateError.generate_type_error(
            val_rule ='num',
            row_num=3,
            attribute_name = 'Check Num', 
            invalid_entry = 'c') in errors
        assert GenerateError.generate_type_error(
            val_rule ='num',
            row_num =4,
             attribute_name = 'Check Num', 
             invalid_entry = '6.5') in errors

        assert GenerateError.generate_type_error(
            val_rule ='int',
            row_num =2,
             attribute_name = 'Check Int', 
             invalid_entry = 7.0) in errors
        assert GenerateError.generate_type_error(
            val_rule ='int',
            row_num =3,
             attribute_name = 'Check Int', 
             invalid_entry = 5.63) in errors
        assert GenerateError.generate_type_error(
            val_rule ='int',
            row_num =4,
             attribute_name = 'Check Int', 
             invalid_entry = 2.0) in errors

        assert GenerateError.generate_list_error(
            list_string='invalid list values',
            row_num='3',
            attribute_name='Check List',
            list_error="not_comma_delimited",
            invalid_entry='invalid list values') in errors

        assert GenerateError.generate_list_error(
            list_string='ab cd ef',
            row_num='3',
            attribute_name='Check Regex List',
            list_error="not_comma_delimited",
            invalid_entry='ab cd ef') in errors

        assert GenerateError.generate_regex_error(
            val_rule='regex',
            reg_expression='[a-f]',
            row_num='3',
            attribute_name='Check Regex Single',
            module_to_call='search',
            invalid_entry='q') in errors   

        assert GenerateError.generate_url_error(
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/') in errors

        


