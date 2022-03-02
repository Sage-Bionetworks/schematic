import os
import logging
import pytest

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.models.validate_manifest import validate_all
from schematic.models.metadata import MetadataModel

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestManifestValidation:
    def test_valid_manifest(self,helpers):
        manifestPath = helpers.get_data_path("mock_manifests/valid_test_manifest.csv")
        rootNode='MockComponent'

        metadataModel= MetadataModel(
            inputMModelLocation =   helpers.get_data_path("example.model.jsonld"),
            inputMModelLocationType="local"
            )

        errors = MetadataModel.validateModelManifest(
            metadataModel,
            manifestPath=manifestPath,
            rootNode=rootNode)
        
        assert errors == [[]]

        
        


    def test_invalid_manifest(self,helpers):
        manifestPath = helpers.get_data_path("mock_manifests/invalid_test_manifest.csv")
        rootNode='MockComponent'


        metadataModel= MetadataModel(
            inputMModelLocation =   helpers.get_data_path("example.model.jsonld"),
            inputMModelLocationType="local"
            )

        errors = MetadataModel.validateModelManifest(
            metadataModel, 
            manifestPath=manifestPath,
            rootNode=rootNode)

        assert len(errors) == 10

        assert GenerateError.generate_type_error('num',2,'Check Num', '6') in errors
        assert GenerateError.generate_type_error('num',3,'Check Num', 'c') in errors
        assert GenerateError.generate_type_error('num',4,'Check Num', '6.5') in errors

        assert GenerateError.generate_type_error('int',2,'Check Int', 7.0) in errors
        assert GenerateError.generate_type_error('int',3,'Check Int', 5.63) in errors
        assert GenerateError.generate_type_error('int',4,'Check Int', 2.0) in errors

        assert GenerateError.generate_list_error('invalid list values',
            '3',
            'Check List',
            "not_comma_delimited",
            'invalid list values') in errors

        assert GenerateError.generate_list_error('ab cd ef',
            '3',
            'Check Regex List',
            "not_comma_delimited",
            'ab cd ef') in errors

        assert GenerateError.generate_url_error(
            'http://googlef.com/',
            'invalid_url',
            '3',
            'Check URL',
            None,
            'http://googlef.com/') in errors

        


