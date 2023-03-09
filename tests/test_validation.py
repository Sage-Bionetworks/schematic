import os
import logging
import re
import jsonschema
import pytest
from pathlib import Path
import itertools

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.models.validate_manifest import ValidateManifest
from schematic.models.metadata import MetadataModel
from schematic.store.synapse import SynapseStorage
from schematic.schemas.generator import SchemaGenerator
from schematic.utils.validate_rules_utils import validation_rule_info
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
            row_num = '3',
            attribute_name = 'Check Num', 
            invalid_entry = 'c',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = '3',
            attribute_name = 'Check Int', 
            invalid_entry = '5.63',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'str',
            row_num = '3',
            attribute_name = 'Check String', 
            invalid_entry = '94',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'invalid list values',
            row_num = '3',
            attribute_name = 'Check List',
            list_error = "not_comma_delimited",
            invalid_entry = 'invalid list values',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'ab cd ef',
            row_num = '3',
            attribute_name = 'Check Regex List',
            list_error = "not_comma_delimited",
            invalid_entry = 'ab cd ef',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Format',
            module_to_call = 'match',
            invalid_entry = 'm',
            sg = sg,
            )[0] in errors   

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Single',
            module_to_call = 'search',
            invalid_entry = 'q',
            sg = sg,
            )[0] in errors   

        assert GenerateError.generate_url_error(
            val_rule = 'url',
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_content_error(
            val_rule = 'date',
            attribute_name = 'Check Date',
            sg = sg,
            row_num = ['2','3','4'],
            error_val = ['84-43-094', '32-984', 'notADate'],
            )[0] in errors

        assert GenerateError.generate_content_error(
            val_rule = 'unique error', 
            attribute_name = 'Check Unique',
            sg = sg,
            row_num = ['2','3','4'],
            error_val = ['str1'],  
            )[0] in errors

        assert GenerateError.generate_content_error(
            val_rule = 'inRange 50 100 error', 
            attribute_name = 'Check Range',
            sg = sg,
            row_num = ['3'],
            error_val = ['30'], 
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
            row_num = ['2','3'],
            error_val = ['6549','32851'], 
            )[1] in warnings

        assert GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne',
            row_num = '[3]',
            attribute_name='Check Match at Least',
            invalid_entry = '[7163]',
            missing_manifest_ID = ['syn27600110', 'syn29381803'],
            sg = sg,
            )[1] in warnings

        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne MockComponent.checkMatchatLeastvalues value',
            row_num = '[3]',
            attribute_name = 'Check Match at Least values',
            invalid_entry = ['51100'],
            sg = sg,
            )[1] in warnings      

        assert \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862078', 'syn27648165'],
            sg = sg,
            )[1] in warnings \
            or \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862066', 'syn27648165'],
            sg = sg,
            )[1] in warnings
                    
        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value',
            row_num = ['2', '3', '4'],
            attribute_name='Check Match Exactly values',
            invalid_entry = ['71738', '98085', '210065'],
            sg = sg,
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
            invalid_entry = 'c',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'int',
            row_num = '3',
            attribute_name = 'Check Int', 
            invalid_entry = '5.63',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_type_error(
            val_rule = 'str',
            row_num = '3',
            attribute_name = 'Check String', 
            invalid_entry = '94',
            sg = sg,
            )[0] in errors
            
        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'invalid list values',
            row_num = '3',
            attribute_name = 'Check List',
            list_error = "not_comma_delimited",
            invalid_entry = 'invalid list values',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_list_error(
            val_rule = 'list strict',
            list_string = 'ab cd ef',
            row_num = '3',
            attribute_name = 'Check Regex List',
            list_error = "not_comma_delimited",
            invalid_entry = 'ab cd ef',
            sg = sg,
            )[0] in errors

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Single',
            module_to_call = 'search',
            invalid_entry = 'q',
            sg = sg,
            )[0] in errors 

        assert GenerateError.generate_regex_error(
            val_rule = 'regex',
            reg_expression = '[a-f]',
            row_num = '3',
            attribute_name = 'Check Regex Format',
            module_to_call = 'match',
            invalid_entry = 'm',
            sg = sg,
            )[0] in errors     

        assert GenerateError.generate_url_error(
            val_rule = 'url',
            url = 'http://googlef.com/',
            url_error = 'invalid_url',
            row_num = '3',
            attribute_name = 'Check URL',
            argument = None,
            invalid_entry = 'http://googlef.com/',
            sg = sg,
            )[0] in errors

        
        #Check Warnings
        assert GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne',
            row_num = ['3'],
            attribute_name='Check Match at Least',
            invalid_entry = ['7163'],
            missing_manifest_ID = ['syn27600110', 'syn29381803'],
            sg = sg,
            )[1] in warnings

        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchAtLeastOne MockComponent.checkMatchatLeastvalues value',
            row_num = ['3'],
            attribute_name = 'Check Match at Least values',
            invalid_entry = ['51100'],
            sg = sg,
            )[1] in warnings      

        assert \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862078', 'syn27648165'],
            sg = sg,
            )[1] in warnings \
            or \
            GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne',
            attribute_name='Check Match Exactly',
            matching_manifests = ['syn29862066', 'syn27648165'],
            sg = sg,
            )[1] in warnings
                    
        assert  GenerateError.generate_cross_warning(
            val_rule = 'matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value',
            row_num = ['2', '3', '4'],
            attribute_name='Check Match Exactly values',
            invalid_entry = ['71738', '98085', '210065'],
            sg = sg,
            )[1] in warnings 
        

    @pytest.mark.rule_combos(reason = 'This introduces a great number of tests covering every possible rule combination that are only necessary on occasion.')
    @pytest.mark.parametrize("base_rule, second_rule", get_rule_combinations())
    def test_rule_combinations(self, helpers, sg, base_rule, second_rule, metadataModel):
        #print(base_rule,second_rule)
        rule_regex = re.compile(base_rule+'.*')

        manifestPath = helpers.get_data_path("mock_manifests/Rule_Combo_Manifest.csv")
        manifest = helpers.get_data_frame(manifestPath)

        for attribute in sg.se.schema['@graph']: #Doing it in a loop becasue of sg.se.edit_class design
            if 'sms:validationRules' in attribute and attribute['sms:validationRules']: 
                if base_rule in attribute['sms:validationRules'] or re.match(rule_regex, attribute['sms:validationRules'][0]):
                    
                    #Add rule args if necessary
                    if second_rule.startswith('matchAtLeastOne') or second_rule.startswith('matchExactlyOne'):
                        rule_args = f" MockComponent.{attribute['rdfs:label']} Patient.PatientID"
                    elif second_rule.startswith('inRange'):
                        rule_args = ' 1 1000 warning'
                    elif second_rule.startswith('regex'):
                        rule_args = ' search [a-f]'
                    else:
                        rule_args = ''
            
                    attribute['sms:validationRules'].append(second_rule + rule_args)
                    sg.se.edit_class(attribute)
                    break

        target_column=attribute['sms:displayName']
        for col in manifest.columns:
            if col not in ('Component', target_column):
                manifest.drop(columns=col, inplace=True)

        rootNode = 'MockComponent'
        validateManifest = ValidateManifest(
            errors = [],
            manifest = manifest,
            manifestPath = manifestPath,
            sg = sg,
            jsonSchema = sg.get_json_schema_requirements(rootNode, rootNode + "_validation")
        )
        
        #perform validation with no exceptions raised
        _, errors, warnings = validateManifest.validate_manifest_rules(
            manifest = manifest, 
            sg =  sg,
            restrict_rules = False,
            project_scope = None,
            )


        
        