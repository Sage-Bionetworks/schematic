import json
from statistics import mode
from tabnanny import check
from jsonschema import Draft7Validator, exceptions, ValidationError
import logging

import numpy as np
import pandas as pd
import re
import sys

# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text, List
from urllib.parse import urlparse
from urllib.request import urlopen, OpenerDirector, HTTPDefaultErrorHandler
from urllib.request import Request
from urllib import error

from schematic.models.validate_attribute import ValidateAttribute
from schematic.schemas.generator import SchemaGenerator

from ruamel import yaml
from ruamel.yaml import YAML

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest, BatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext

logger = logging.getLogger(__name__)


class ValidateManifest(object):
    def __init__(self, errors, manifest, sg, jsonSchema):
        self.errors = errors
        self.manifest = manifest
        self.sg = sg
        self.jsonSchema = jsonSchema
        

    def  build_validator(self):
        
        self.context=ge.get_context()
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
                    #"module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }


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
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/great_expectations"),
        )

        self.context=BaseDataContext(project_config=data_context_config)

        #self.context.test_yaml_config(yaml.dump(datasource_config))
        self.context.add_datasource(**datasource_config)
        
        '''
        self.batch_request = RuntimeBatchRequest(
            datasource_name="example_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="Manifest",  # This can be anything that identifies this data_asset for you
            runtime_parameters={"batch_data": self.manifest},  # df is your dataframe
            batch_identifiers={"default_identifier_name": "default_identifier"},

        )
           
    
        self.context.create_expectation_suite(
            expectation_suite_name="Manifest_test_suite", overwrite_existing=True
        )

            
        self.validator = self.context.get_validator(
            batch_request=self.batch_request, expectation_suite_name="Manifest_test_suite"
        )
        

        print(self.validator.head())
        '''
        
    def build_expectation_suite(self, sg: SchemaGenerator):

        validation_expectation = {
            "int": "expect_column_values_to_be_of_type",
            "float": "expect_column_values_to_be_of_type",
            "str": "expect_column_values_to_be_of_type",
            "num": "expect_column_values_to_be_in_type_list",
            "regex": "expect_column_values_to_match_regex",
            #"url": "expect_column_values_to_be_valid_urls",
            #"list": "expect_column_values_to_follow_rule",
            "list": "expect_column_values_to_be_of_type",
            "regexList": "expect_column_values_to_match_regex_list",
        }

        expectationSuiteName = "Manifest_test_suite"
        '''
        try:
            suite = self.context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
            print(
                f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
            )
        except DataContextError:
        '''
        suite = self.context.create_expectation_suite(
            expectation_suite_name=expectationSuiteName,
            overwrite_existing=True
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')        


        #validation_rules_and_col={}

        
        for col in self.manifest.columns:
            # remove trailing/leading whitespaces from manifest
            self.manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            rule=sg.get_node_validation_rules(col)[0]
            """
            if sg.get_node_validation_rules(col)[0] not in validation_rules_and_col.keys():
                validation_rules_and_col[sg.get_node_validation_rules(col)[0]]=[]
            
            validation_rules_and_col[sg.get_node_validation_rules(col)[0]].append(str(col))
            """

            if rule in ['url']:
                continue

            args={}
            args["column"]=col

            #print(rule)

            #Validate lift of regices
            if len(sg.get_node_validation_rules(col)) > 1:
               
                
                args["mostly"]=1.0
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectation {validation_expectation[rule]} **Markdown** `Supported`"
                    }
                }
                
            #Validate list
            elif rule=='list':
                args["mostly"]=1.0
                args["type_"]="list"
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectat column values to be list type **Markdown** `Supported`"
                    }
                }
           
            #Validate regex
            elif rule.startswith('regex'):
                
                args["mostly"]=1.0
                args["regex"]=rule.split(" ")[-1]
                rule='regex'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectat column values to match regex  **Markdown** `Supported`"
                    }
                }
           
            #Validate url
            elif rule=='url':
                args["mostly"]=1.0
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expectat URLs in column to be valid. **Markdown** `Supported`"
                    }
                }
           
            #Validate num
            elif rule=='num':
                args["mostly"]=1.0
                args["type_list"]=['int', "float"]
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of int or float type. **Markdown** `Supported`"
                    }
                }
           
            #Validate float
            elif rule=='float':
                args["mostly"]=1.0
                args["type_"]='float'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of float type. **Markdown** `Supported`"
                    }
                }
           
            #Validate int
            elif rule=='int':
                args["mostly"]=1.0
                args["type_"]='int'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of int type. **Markdown** `Supported`"
                    }
                }
           
            #Validate string
            elif rule=='str':
                args["mostly"]=1.0
                args["type_"]='str'
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be of string type. **Markdown** `Supported`"
                    }
                }
        
            # Create an Expectation
            expectation_configuration = ExpectationConfiguration(
                # Name of expectation type being added
                expectation_type=validation_expectation[rule],

                # These are the arguments of the expectation
                # The keys allowed in the dictionary are Parameters and
                # Keyword Arguments of this Expectation Type
                kwargs={**args},

                # This is how you can optionally add a comment about this expectation.
                # It will be rendered in Data Docs.
                # See this guide for details:
                # `How to add comments to Expectations and display them in Data Docs`.
                meta={**meta}
            )
            # Add the Expectation to the suite
            suite.add_expectation(expectation_configuration=expectation_configuration)

        
        #print(self.context.get_expectation_suite(expectation_suite_name=expectationSuiteName))
        self.context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=expectationSuiteName)

        suite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=expectationSuiteName)
        self.context.build_data_docs(resource_identifiers=[suite_identifier])
        #self.context.open_data_docs(resource_identifier=suite_identifier)

        #print(suite)
            

    def build_checkpoint(self):
        #print(self.validator.get_expectation_suite(discard_failed_expectations=False))
        #self.validator.save_expectation_suite(discard_failed_expectations=False)
        checkpoint_name = "manifest_checkpoint"  

        yaml_config = f"""
        name: {checkpoint_name}
        config_version: 1.0
        class_name: SimpleCheckpoint
        run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
        validations:
        - batch_request:
            datasource_name: example_datasource
            data_connector_name: default_runtime_data_connector_name
            data_asset_name: Manifest
            data_connector_query:
                index: -1
        expectation_suite_name: Manifest_test_suite
        """

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
 

        #print(yaml_config)

        self.context.test_yaml_config(yaml.dump(checkpoint_config),return_mode="report_object")
        

        #self.context.add_checkpoint(**YAML().load(yaml_config))
        self.context.add_checkpoint(**checkpoint_config)
        pass


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

        self.build_validator()
        self.build_expectation_suite(sg)
        self.build_checkpoint()
        #print(self.context.get_checkpoint(name='manifest_checkpoint'))
        #print(self.batch_request)
       
        results = self.context.run_checkpoint(
            checkpoint_name="manifest_checkpoint",
            batch_request={
                "runtime_parameters": {"batch_data": manifest},
                "batch_identifiers": {
                    "default_identifier_name": "default_identifier"
                },
            },
        )        
        
        


        '''
        result = self.context.run_checkpoint(
                checkpoint_name='manifest_checkpoint',                
                batch_request={
                    "runtime_parameters": {"batch_data": manifest},
                    "batch_identifiers": {
                        "default_identifier_name": "default_identifier"
                    }
                },               
            )
            
        '''
        print(results)
        
        


        errors = []  # initialize error handling list.
        for col in manifest.columns:
            # remove trailing/leading whitespaces from manifest
            manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            validation_rules = sg.get_node_validation_rules(col)

            # Given a validation rule, run validation.
            if bool(validation_rules):

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
