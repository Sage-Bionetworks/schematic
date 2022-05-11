import os
import logging
import pytest
from pathlib import Path

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.models.validate_manifest import validate_all
from schematic.models.metadata import MetadataModel
from schematic.store.synapse import SynapseStorage
from schematic.schemas.generator import SchemaGenerator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def sg(helpers):

    inputModelLocation = helpers.get_data_path('example.model.jsonld')
    sg = SchemaGenerator(inputModelLocation)

    yield sg

@pytest.fixture
def metadataModel(helpers):
    metadataModel = MetadataModel(
        inputMModelLocation = helpers.get_data_path("example.model.jsonld"),
        inputMModelLocationType = "local"
        )

    yield metadataModel

class TestManifestValidation:
    def test_valid_manifest(self,helpers,metadataModel):
        manifestPath = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
        rootNode = 'MockComponent'

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode
            )
        
        assert errors == []
        assert warnings ==  []


    def test_invalid_manifest(self,helpers,sg,metadataModel):
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = 'MockComponent'

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode
            )

        #Check errors
        assert GenerateError.generate_type_error(
            val_rule = 'num',
            row_num = 3,
            attribute_name = 'Check Num', 
            invalid_entry = 'c'
            ) in errors

        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = 3,
            attribute_name = 'Check Int', 
            invalid_entry = 5.63
            ) in errors

        assert GenerateError.generate_list_error(
            list_string = 'invalid list values',
            row_num = '3',
            attribute_name = 'Check List',
            list_error = "not_comma_delimited",
            invalid_entry = 'invalid list values'
            ) in errors

        assert GenerateError.generate_list_error(
            list_string = 'ab cd ef',
            row_num = '3',
            attribute_name = 'Check Regex List',
            list_error = "not_comma_delimited",
            invalid_entry = 'ab cd ef'
            ) in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Single',
            module_to_call = 'search',
            invalid_entry = 'q'
            ) in errors   

        assert GenerateError.generate_url_error(
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/'
            ) in errors

        assert GenerateError.generate_cross_error(
            val_rule = 'matchAtLeastOne',
            row_num = '3',
            attribute_name='checkMatchatLeast',
            missing_entry = '7163',
            missing_manifest_ID = 'syn27600110',
            ) in errors

        assert GenerateError.generate_cross_error(
            val_rule = 'matchAtLeastOne',
            row_num = '3',
            attribute_name='checkMatchatLeast',
            missing_entry = '7163',
            missing_manifest_ID = 'syn29381803',
            ) in errors

        assert \
            GenerateError.generate_cross_error(
            val_rule = 'matchExactlyOne',
            attribute_name='checkMatchExactly',
            matching_manifests = ['syn29862078', 'syn27648165']
            ) in errors \
            or \
            GenerateError.generate_cross_error(
            val_rule = 'matchExactlyOne',
            attribute_name='checkMatchExactly',
            matching_manifests = ['syn29862066', 'syn27648165']
            ) in errors
                    
        assert GenerateError.generate_content_error(
            val_rule = 'unique error', 
            attribute_name = 'Check Unique',
            sg = sg,
            row_num = [2,3,4],
            error_val = ['str1'],  
            )[0] in errors

        assert GenerateError.generate_content_error(
            val_rule = 'inRange 50 100 error', 
            attribute_name = 'Check Range',
            sg = sg,
            row_num = [3],
            error_val = [30], 
            )[0] in errors

        #check warnings
        assert GenerateError.generate_content_error(
            val_rule = 'recommended', 
            attribute_name = 'Check Recommended',
            sg = sg,
            )[1] in warnings
        
        assert GenerateError.generate_content_error(
            val_rule = 'protectAges', 
            attribute_name = 'Check Ages',
            sg = sg,
            row_num = [2,3],
            error_val = [6549,32851], 
            )[1] in warnings
        

    def test_in_house_validation(self,helpers,sg,metadataModel):
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = 'MockComponent'

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            restrict_rules=True,
            )  

        #Check errors
        assert GenerateError.generate_type_error(
            val_rule = 'num',
            row_num = '3',
            attribute_name = 'Check Num', 
            invalid_entry = 'c'
            ) in errors

        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = '3',
            attribute_name = 'Check Int', 
            invalid_entry = '5.63'
            ) in errors

        assert GenerateError.generate_list_error(
            list_string = 'invalid list values',
            row_num = '3',
            attribute_name = 'Check List',
            list_error = "not_comma_delimited",
            invalid_entry = 'invalid list values'
            ) in errors

        assert GenerateError.generate_list_error(
            list_string = 'ab cd ef',
            row_num = '3',
            attribute_name = 'Check Regex List',
            list_error = "not_comma_delimited",
            invalid_entry = 'ab cd ef'
            ) in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Single',
            module_to_call = 'search',
            invalid_entry = 'q'
            ) in errors   

        assert GenerateError.generate_url_error(
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/'
            ) in errors

        assert GenerateError.generate_cross_error(
            val_rule = 'matchAtLeastOne',
            row_num = '3',
            attribute_name='checkMatchatLeast',
            missing_entry = '7163',
            missing_manifest_ID = 'syn27600110',
            ) in errors

        assert GenerateError.generate_cross_error(
            val_rule = 'matchAtLeastOne',
            row_num = '3',
            attribute_name='checkMatchatLeast',
            missing_entry = '7163',
            missing_manifest_ID = 'syn29381803',
            ) in errors

        assert \
            GenerateError.generate_cross_error(
            val_rule = 'matchExactlyOne',
            attribute_name='checkMatchExactly',
            matching_manifests = ['syn29862078', 'syn27648165']
            ) in errors \
            or \
            GenerateError.generate_cross_error(
            val_rule = 'matchExactlyOne',
            attribute_name='checkMatchExactly',
            matching_manifests = ['syn29862066', 'syn27648165']
            ) in errors
        

