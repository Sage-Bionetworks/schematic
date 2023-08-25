import os
import logging
import re
import networkx as nx
import jsonschema
import pytest
from pathlib import Path
import itertools

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.models.validate_manifest import ValidateManifest
from schematic.models.metadata import MetadataModel
from schematic.store.synapse import SynapseStorage

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema

from schematic.utils.validate_rules_utils import validation_rule_info
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def DME(helpers):

    inputModelLocation = helpers.get_data_path('example.model.jsonld')
    #sg = SchemaGenerator(inputModelLocation)
    data_model_parser = DataModelParser(path_to_data_model = inputModelLocation)
    #Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.generate_data_model_graph()

    # Instantiate DataModelGraphExplorer
    DME = DataModelGraphExplorer(graph_data_model)

    yield DME

@pytest.fixture
def metadataModel(helpers):
    metadataModel = MetadataModel(
        inputMModelLocation = helpers.get_data_path("example.model.jsonld"),
        inputMModelLocationType = "local"
        )

    yield metadataModel

def get_rule_combinations():
    rule_info = validation_rule_info()
    for base_rule, indiv_info in rule_info.items():
        complementary_rules = indiv_info['complementary_rules']
        if complementary_rules:
            for second_rule in complementary_rules:
                yield base_rule, second_rule
        else:
            continue
    
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


    def test_invalid_manifest(self,helpers, DME,metadataModel):
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = 'MockComponent'

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode
            )

        #Check errors
        assert GenerateError.generate_type_error(
            val_rule = 'num',
            row_num = '3',
            attribute_name = 'Check Num', 
            invalid_entry = 'c',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = '3',
            attribute_name = 'Check Int', 
            invalid_entry = '5.63',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'str',
            row_num = '3',
            attribute_name = 'Check String', 
            invalid_entry = '94',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'invalid list values',
            row_num = '3',
            attribute_name = 'Check List',
            list_error = "not_comma_delimited",
            invalid_entry = 'invalid list values',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'ab cd ef',
            row_num = '3',
            attribute_name = 'Check Regex List',
            list_error = "not_comma_delimited",
            invalid_entry = 'ab cd ef',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Format',
            module_to_call = 'match',
            invalid_entry = 'm',
            DME = DME,
            )[0] in errors   

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Single',
            module_to_call = 'search',
            invalid_entry = 'q',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '^\d+$',
            row_num = '2',
            attribute_name = 'Check Regex Integer',
            module_to_call = 'search',
            invalid_entry = '5.4',
            DME = DME,
            )[0] in errors 

        assert GenerateError.generate_url_error(
            val_rule = 'url',
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/',
            DME = DME,
            )[0] in errors


        date_err = GenerateError.generate_content_error(
            val_rule = 'date',
            attribute_name = 'Check Date',
            DME = DME,
            row_num = ['2','3','4'],
            error_val = ['84-43-094', '32-984', 'notADate'],
            )[0]
        error_in_list = [date_err[2] in error for error in errors]
        assert any(error_in_list)

        assert GenerateError.generate_content_error(
            val_rule = 'unique error', 
            attribute_name = 'Check Unique',
            DME = DME,
            row_num = ['2','3','4'],
            error_val = ['str1'],  
            )[0] in errors

        assert GenerateError.generate_content_error(
            val_rule = 'inRange 50 100 error', 
            attribute_name = 'Check Range',
            DME = DME,
            row_num = ['3'],
            error_val = ['30'], 
            )[0] in errors

        #check warnings
        assert GenerateError.generate_content_error(
            val_rule = 'recommended', 
            attribute_name = 'Check Recommended',
            DME = DME,
            )[1] in warnings
        
        assert GenerateError.generate_content_error(
            val_rule = 'protectAges', 
            attribute_name = 'Check Ages',
            DME = DME,
            row_num = ['2','3'],
            error_val = ['6549','32851'], 
            )[1] in warnings

        assert GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne',
            row_num = ['3'],
            attribute_name='Check Match at Least',
            invalid_entry = ['7163'],
            missing_manifest_ID = ['syn27600110', 'syn29381803'],
            DME = DME,
            )[1] in warnings

        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne MockComponent.checkMatchatLeastvalues value',
            row_num = ['3'],
            attribute_name = 'Check Match at Least values',
            invalid_entry = ['51100'],
            DME = DME,
            )[1] in warnings      

        assert \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862078', 'syn27648165'],
            DME = DME,
            )[1] in warnings \
            or \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862066', 'syn27648165'],
            DME = DME,
            )[1] in warnings


        cross_warning = GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value',
            row_num = ['2', '3', '4'],
            attribute_name='Check Match Exactly values',
            invalid_entry = ['71738', '98085', '210065'],
            DME = DME,
            )[1]
        warning_in_list = [cross_warning[1] in warning for warning in warnings]
        assert any(warning_in_list)

        
        

    def test_in_house_validation(self,helpers,DME,metadataModel):
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
            invalid_entry = 'c',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = '3',
            attribute_name = 'Check Int', 
            invalid_entry = '5.63',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'str',
            row_num = '3',
            attribute_name = 'Check String', 
            invalid_entry = '94',
            DME = DME,
            )[0] in errors
        
        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = '3',
            attribute_name = 'Check NA', 
            invalid_entry = '9.5',
            DME = DME,
            )[0] in errors
            
        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'invalid list values',
            row_num = '3',
            attribute_name = 'Check List',
            list_error = "not_comma_delimited",
            invalid_entry = 'invalid list values',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'ab cd ef',
            row_num = '3',
            attribute_name = 'Check Regex List',
            list_error = "not_comma_delimited",
            invalid_entry = 'ab cd ef',
            DME = DME,
            )[0] in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Single',
            module_to_call = 'search',
            invalid_entry = 'q',
            DME = DME,
            )[0] in errors 

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Format',
            module_to_call = 'match',
            invalid_entry = 'm',
            DME = DME,
            )[0] in errors     

        assert GenerateError.generate_url_error(
            val_rule = 'url',
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/',
            DME = DME,
            )[0] in errors

        
        #Check Warnings
        assert GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne',
            row_num = ['3'],
            attribute_name='Check Match at Least',
            invalid_entry = ['7163'],
            missing_manifest_ID = ['syn27600110', 'syn29381803'],
            DME = DME,
            )[1] in warnings

        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne MockComponent.checkMatchatLeastvalues value',
            row_num = ['3'],
            attribute_name = 'Check Match at Least values',
            invalid_entry = ['51100'],
            DME = DME,
            )[1] in warnings      

        assert \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862078', 'syn27648165'],
            DME = DME,
            )[1] in warnings \
            or \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862066', 'syn27648165'],
            DME = DME,
            )[1] in warnings
                    
        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value',
            row_num = ['2', '3', '4'],
            attribute_name='Check Match Exactly values',
            invalid_entry = ['71738', '98085', '210065'],
            DME = DME,
            )[1] in warnings 
        

    @pytest.mark.rule_combos(reason = 'This introduces a great number of tests covering every possible rule combination that are only necessary on occasion.')
    @pytest.mark.parametrize("base_rule, second_rule", get_rule_combinations())
    def test_rule_combinations(self, helpers, DME, base_rule, second_rule, metadataModel):
        """
        TODO: Describe what this test is doing.
        Updating the data model graph to allow testing of allowable rule combinations.
        Works one rule combo at a time using (get_rule_combinations.)
        """
        rule_regex = re.compile(base_rule+'.*')
        rootNode = 'MockComponent'

        manifestPath = helpers.get_data_path("mock_manifests/Rule_Combo_Manifest.csv")
        manifest = helpers.get_data_frame(manifestPath)
        
        # Get a view of the node data
        all_node_data = DME.graph.nodes.data()
        
        # Update select validation rules in the data model graph for columns in the manifest
        for attribute in manifest.columns:
            # Get the node label
            node_label = DME.get_node_label(attribute)
            
            # Get a view of the recorded info for current node
            node_info = all_node_data[node_label]
            if node_info['validationRules']:
                
                if node_info['displayName'] == 'Check NA':
                    # Edit the node info -in place-
                    node_info['validationRules'].remove('int')
                    break
                
                if base_rule in node_info['validationRules'] or re.match(rule_regex, node_info['validationRules'][0]):
                    if second_rule.startswith('matchAtLeastOne') or second_rule.startswith('matchExactlyOne'):
                        rule_args = f" MockComponent.{node_label} Patient.PatientID"
                    elif second_rule.startswith('inRange'):
                        rule_args = ' 1 1000 warning'
                    elif second_rule.startswith('regex'):
                        rule_args = ' search [a-f]'
                    else:
                        rule_args = ''
                    # Edit the node info -in place-
                    node_info['validationRules'].append(second_rule + rule_args)
                    break

        # Update the manifest to only contain the Component and attribute column where the rule was changed.        
        manifest = manifest[['Component', attribute]]
         
        data_model_js = DataModelJSONSchema(jsonld_path=helpers.get_data_path('example.model.jsonld'), graph=DME.graph)
        json_schema = data_model_js.get_json_validation_schema(source_node=rootNode, schema_name=rootNode + "_validation")

        validateManifest = ValidateManifest(
            errors = [],
            manifest = manifest,
            manifestPath = manifestPath,
            DME = DME,
            jsonSchema = json_schema
        )
        
        #perform validation with no exceptions raised
        _, errors, warnings = validateManifest.validate_manifest_rules(
            manifest = manifest, 
            DME = DME,
            restrict_rules = False,
            project_scope = None,
            )


        
        