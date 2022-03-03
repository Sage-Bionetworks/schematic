import json
from statistics import mode
from tabnanny import check
from jsonschema import Draft7Validator, exceptions, ValidationError
import logging

import numpy as np
import os
import pandas as pd
import re
import sys

# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text, List
from urllib.parse import urlparse
from urllib.request import urlopen, OpenerDirector, HTTPDefaultErrorHandler
from urllib.request import Request
from urllib import error

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.schemas.generator import SchemaGenerator
from schematic.store.synapse import SynapseStorage
from schematic.store.base import BaseStorage

import synapseclient
syn=synapseclient.Synapse()

#from ruamel import yaml

import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier

logger = logging.getLogger(__name__)


class ValidateManifest(object):
    def __init__(self, errors, manifest, sg, jsonSchema):
        self.errors = errors
        self.manifest = manifest
        self.sg = sg
        self.jsonSchema = jsonSchema
        

    def  build_context(self):
        self.context=ge.get_context()

        #create datasource configuration
        datasource_config = {
            "name": "example_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }

        #create data context configuration
        data_context_config = DataContextConfig(
            datasources={
                "pandas": DatasourceConfig(
                    class_name="Datasource",
                    execution_engine={
                        "class_name": "PandasExecutionEngine"
                    },
                    data_connectors={
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["default_identifier_name"],
                        }
                    },
                )
            },
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=os.path.join(os.getcwd(),'great_expectations')),
        )

        #build context and add data source
        self.context=BaseDataContext(project_config=data_context_config)
        #self.context.test_yaml_config(yaml.dump(datasource_config))
        self.context.add_datasource(**datasource_config)
        
        
    def build_expectation_suite(self, sg: SchemaGenerator, unimplemented_expectations = []):
        validation_expectation = {
            "int": "expect_column_values_to_be_of_type",
            "float": "expect_column_values_to_be_of_type",
            "str": "expect_column_values_to_be_of_type",
            #"int": "expect_column_values_to_be_in_type_list",
            #"float": "expect_column_values_to_be_in_type_list",            
            #"str": "expect_column_values_to_be_in_type_list",
            "num": "expect_column_values_to_be_in_type_list",
            "regex": "expect_column_values_to_match_regex",
            "url": "expect_column_values_to_be_valid_urls",
            "list": "expect_column_values_to_follow_rule",
            "matchAtLeastOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
            "matchExactlyOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
        }
        
        #create blank expectation suite
        expectation_suite_name = "Manifest_test_suite"       
        suite = self.context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=True
        )
        #print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')        

        #build expectation configurations for each expecation
        for col in self.manifest.columns:

            # remove trailing/leading whitespaces from manifest
            self.manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            rule=sg.get_node_validation_rules(col)[0]
            
            args={}
            meta={}

            print(rule)
            #update to only do regex match
            if rule in unimplemented_expectations or (rule.startswith('regex') and not rule.__contains__('match')) or (rule.startswith('matchExactlyOne') or rule.startswith('matchAtLeastOne')): #modify if list is implemented before list::regex
                continue

            
            args["column"]=col
            args["result_format"] = "COMPLETE"


            #Validate lift of regices
            if len(sg.get_node_validation_rules(col)) > 1: #currently unused
                args["mostly"]=1.0
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectation {validation_expectation[rule]} **Markdown** `Supported`"
                    },
                    "validation_rule": rule
                }
                
            #Validate list
            elif rule=='list':  #currently unused
                args["mostly"]=1.0
                args["type_"]="list"
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectat column values to be list type **Markdown** `Supported`"
                    },
                    "validation_rule": rule
                }
           
            #Validate regex
            elif rule.startswith('regex match'):
                
                args["mostly"]=1.0
                args["regex"]=rule.split(" ")[-1]
                rule='regex'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectat column values to match regex  **Markdown** `Supported`"
                    },
                    "validation_rule": rule
                }
           
            #Validate url
            elif rule=='url': #currently unused
                args["mostly"]=1.0
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectat URLs in column to be valid. **Markdown** `Supported`"
                    },
                    "validation_rule": rule
                }
           
            #Validate num
            elif rule=='num':
                args["mostly"]=1.0
                args["type_list"]=['int64', "float64"]
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of int or float type. **Markdown** `Supported`"
                    },
                    "validation_rule": rule
                }
           
            #Validate float
            elif rule=='float':
                args["mostly"]=1.0
                args["type_"]='float64'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of float type. **Markdown** `Supported`",
                    },
                    "validation_rule": rule
                }
           
            #Validate int
            elif rule=='int':
                args["mostly"]=1.0
                args["type_"]='int64' 
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of int type. **Markdown** `Supported`",
                    },
                    "validation_rule": rule
                }
           
            #Validate string
            elif rule=='str':
                args["mostly"]=1.0
                args["type_"]='str'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of string type. **Markdown** `Supported`",
                    },
                    "validation_rule": rule
                }

            #validate cross manifest match
            elif rule.startswith("matchAtLeastOne" or "matchExactlyOne"):
                
                
                [source_component, source_attribute] = rule.split(" ")[1].split(".")
                [target_component, target_attribute] = rule.split(" ")[2].split(".")

                #Find all manifests with 2nd component
                synStore = SynapseStorage()
                synStore.login()
                syn.login()
                projects = synStore.getStorageProjects()
                for project in projects:
                    print(project[0])
                    
                    target_datasets=synStore.getProjectManifests(projectId=project[0])
                    print(synStore.getProjectManifests(projectId=project[0]))

                    for target_dataset in target_datasets:
                        print(target_dataset)
                        if target_component in target_dataset[-1]:
                            target_manifest_ID = target_dataset[1][0]

                            entity = syn.get(target_manifest_ID)
                            target_manifest=pd.read_csv(entity.path)
                            if target_attribute in target_manifest.columns:
                                target_column = target_manifest[target_attribute]

                                #Do the validation on both columns

                                #Write expectation to maybe take multiple columns 
                                #instead of just one B
                        
                            else:
                                print("Attribute not found in manifest")
                                continue                           
                        


        
            # Create an Expectation, move to its own function
            expectation_configuration = ExpectationConfiguration(
                # Name of expectation type being added
                expectation_type=validation_expectation[rule],

                #add arguments and meta message
                kwargs={**args},
                meta={**meta}
            )
            # Add the Expectation to the suite
            suite.add_expectation(expectation_configuration=expectation_configuration)

        
        #print(self.context.get_expectation_suite(expectation_suite_name=expectation_suite_name))
        self.context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=expectation_suite_name)

        suite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)
        self.context.build_data_docs(resource_identifiers=[suite_identifier])
        #self.context.open_data_docs(resource_identifier=suite_identifier) #Webpage DataDocs opened here


    def build_checkpoint(self):

        #create manifest checkpoint
        checkpoint_name = "manifest_checkpoint"  
        checkpoint_config={
            "name": checkpoint_name,
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "example_datasource",
                        "data_connector_name": "default_runtime_data_connector_name",
                        "data_asset_name": "Manifest",
                    },
                    "expectation_suite_name": "Manifest_test_suite",
                }
            ],
        }

        #self.context.test_yaml_config(yaml.dump(checkpoint_config),return_mode="report_object")        
        self.context.add_checkpoint(**checkpoint_config)



    def get_multiple_types_error(
        validation_rules: list, attribute_name: str, error_type: str
    ) -> List[str]:
        """
            Generate error message for errors when trying to specify 
            multiple validation rules.
            """
        error_col = attribute_name  # Attribute name
        if error_type == "too_many_rules":
            error_str = (
                f"For attribute {attribute_name}, the provided validation rules ({validation_rules}) ."
                f"have too many entries. We currently only specify two rules ('list :: another_rule')."
            )
            logging.error(error_str)
            error_message = error_str
            error_val = f"Multiple Rules: too many rules"
        if error_type == "list_not_first":
            error_str = (
                f"For attribute {attribute_name}, the provided validation rules ({validation_rules}) are improperly "
                f"specified. 'list' must be first."
            )
            logging.error(error_str)
            error_message = error_str
            error_val = f"Multiple Rules: list not first"
        return ["NA", error_col, error_message, error_val]

    def validate_manifest_rules(
        self, manifest: pd.core.frame.DataFrame, sg: SchemaGenerator
    ) -> (pd.core.frame.DataFrame, List[List[str]]):
        """
        Purpose:
            Take validation rules set for a particular attribute
            and validate manifest entries based on these rules.
        Input:
            manifest: pd.core.frame.DataFrame
                imported from models/metadata.py
                contains metadata input from user for each attribute.
            sg: SchemaGenerator
                initialized within models/metadata.py
        Returns:
            manifest: pd.core.frame.DataFrame
                If a 'list' validatior is run, the manifest needs to be 
                updated to change the attribute column values to a list.
                In this case the manifest will be updated then exported.
            errors: List[List[str]]
                If any errors are generated they will be added to an errors
                list log recording the following information:
                [error_row, error_col, error_message, error_val]
        TODO: 
            -Investigate why a :: delimiter is breaking up the
                validation rules without me having to do anything...
            - Move the rules formatting validation to the JSONLD 
                generation script.
        """

        # for each type of rule that can be spefified (key) point
        # to the type of validation that will be run.
        validation_types = {
            "int": "type_validation",
            "float": "type_validation",
            "num": "type_validation",
            "str": "type_validation",
            "regex": "regex_validation",
            "url": "url_validation",
            "list": "list_validation",
        }

        type_dict={
            'float64': float,
            'int64': int,
            'str': str,
        }

        unimplemented_expectations=['url','regexList','list','regex search']

        #operations necessary to set up and run ge suite validation
        self.build_context()
        self.build_expectation_suite(sg, unimplemented_expectations)
        self.build_checkpoint()

       
       #run GE validation
        results = self.context.run_checkpoint(
            checkpoint_name="manifest_checkpoint",
            batch_request={
                "runtime_parameters": {"batch_data": manifest},
                "batch_identifiers": {
                    "default_identifier_name": "manifestID"
                },
            },
            result_format={'result_format': 'COMPLETE'},
        )        
        #print(results)       
        #results.list_validation_results()

        errors = []  # initialize error handling 2list. 

        #parse validation results dict   
        validation_results = results.list_validation_results()
        for result_dict in validation_results[0]['results']:
            
            indices = []
            values = []

            #print(result_dict)
            #print(result_dict['expectation_config']['expectation_type'])

            #if the expectaion failed, get infromation to generate error message
            if not result_dict['success']:
                errColumn   = result_dict['expectation_config']['kwargs']['column']               
                rule        = result_dict['expectation_config']['meta']['validation_rule']


                #only some expectations explicitly list unexpected values and indices, read or find if not present
                if 'unexpected_index_list' in result_dict['result']:
                    indices = result_dict['result']['unexpected_index_list']
                    values  = result_dict['result']['unexpected_list']

                #because type validation is column aggregate expectation and not column map expectation, indices and values cannot be returned
                else:
                    for i, item in enumerate(manifest[errColumn]):
                        observed_type=result_dict['result']['observed_value']
                        indices.append(i)   if isinstance(item,type_dict[observed_type]) else indices
                        values.append(item) if isinstance(item,type_dict[observed_type]) else values

                #call functions to generate error messages and add to error list
                if validation_types[rule]=='type_validation':
                    for row, value in zip(indices,values):
                        errors.append(
                            GenerateError.generate_type_error(
                                rule, row+2, errColumn, value
                            )
                        )                                      
                elif validation_types[rule]=='regex_validation':
                    expression=result_dict['expectation_config']['kwargs']['regex']

                    for row, value in zip(indices,values):   
                        errors.append(
                            GenerateError.generate_regex_error(
                                rule, expression, row+2, 'match', errColumn, value
                            )
                        )                                      


        for col in manifest.columns:
            # remove trailing/leading whitespaces from manifest
            manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            validation_rules = sg.get_node_validation_rules(col)


            print(validation_rules)
            # Given a validation rule, run validation. Skip validations already performed by GE
            if bool(validation_rules) and validation_rules[0] in unimplemented_expectations or (validation_rules[0]=='list' and validation_rules[1].startswith('regex')):
                # Check for multiple validation types,
                # If there are multiple types, validate them.
                if len(validation_rules) == 2:

                    # For multiple rules check that the first rule listed is 'list'
                    # if not, throw an error (this is the only format currently supported).
                    if not validation_rules[0] == "list":
                        errors.append(
                            get_multiple_types_error(
                                validation_rules, col, error_type="list_not_first"
                            )
                        )
                    elif validation_rules[0] == "list":
                        # Convert user input to list.
                        validation_method = getattr(
                            ValidateAttribute, validation_types["list"]
                        )
                        vr_errors, manifest_col = validation_method(
                            self, validation_rules[0], manifest[col]
                        )
                        manifest[col] = manifest_col

                        # Continue to second validation rule
                        second_rule = validation_rules[1].split(" ")
                        second_type = second_rule[0]
                        if second_type != "list":
                            module_to_call = getattr(re, second_rule[1])
                            if module_to_call == 'match':
                                continue
                            regular_expression = second_rule[2]
                            validation_method = getattr(
                                ValidateAttribute, validation_types[second_type]
                            )
                            vr_errors.append(
                                validation_method(
                                    self, validation_rules[1], manifest[col]
                                )
                            )
                # Check for edge case that user has entered more than 2 rules,
                # throw an error if they have.
                elif len(validation_rules) > 2:
                    get_multiple_types_error(
                        validation_rules, col, error_type="too_many_rules"
                    )

                # Validate for a single validation rule.
                else:
                    validation_type = validation_rules[0].split(" ")[0]
                    validation_method = getattr(
                        ValidateAttribute, validation_types[validation_type]
                    )
                    if validation_type == "list":
                        vr_errors, manifest_col = validation_method(
                            self, validation_rules[0], manifest[col]
                        )
                        manifest[col] = manifest_col
                    else:
                        vr_errors = validation_method(
                            self, validation_rules[0], manifest[col]
                        )
                # Check for validation rule errors and add them to other errors.
                if vr_errors:
                    errors.extend(vr_errors)
        return manifest, errors

    def validate_manifest_values(self, manifest, jsonSchema):
        
        errors = []
        annotations = json.loads(manifest.to_json(orient="records"))
        for i, annotation in enumerate(annotations):
            v = Draft7Validator(jsonSchema)

            for error in sorted(v.iter_errors(annotation), key=exceptions.relevance):
                errorRow = i + 2
                errorCol = error.path[-1] if len(error.path) > 0 else "Wrong schema"
                errorMsg = error.message[0:500]
                errorVal = error.instance if len(error.path) > 0 else "Wrong schema"

                errors.append([errorRow, errorCol, errorMsg, errorVal])
        return errors


def validate_all(self, errors, manifest, sg, jsonSchema):
    vm = ValidateManifest(errors, manifest, sg, jsonSchema)
    manifest, vmr_errors = vm.validate_manifest_rules(manifest, sg)
    if vmr_errors:
        errors.extend(vmr_errors)

    vmv_errors = vm.validate_manifest_values(manifest, jsonSchema)
    if vmv_errors:
        errors.extend(vmv_errors)
    return errors, manifest
