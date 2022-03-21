
from statistics import mode
from tabnanny import check
import logging
import os
import re
import numpy as np

# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text, List
from urllib.parse import urlparse
from urllib.request import urlopen, OpenerDirector, HTTPDefaultErrorHandler
from urllib.request import Request
from urllib import error
from attr import attr

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier

from schematic.models.validate_attribute import GenerateError

logger = logging.getLogger(__name__)

class GreatExpectationsHelpers(object):
    def __init__(self, sg, unimplemented_expectations,manifest):
        self.unimplemented_expectations = unimplemented_expectations
        self.sg = sg
        self.manifest = manifest

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

        
    def build_expectation_suite(self,):
        validation_expectation = {
            "int": "expect_column_values_to_be_of_type",
            "float": "expect_column_values_to_be_of_type",
            "str": "expect_column_values_to_be_of_type",
            "num": "expect_column_values_to_be_in_type_list",
            "regex": "expect_column_values_to_match_regex",
            "url": "expect_column_values_to_be_valid_urls",
            "list": "expect_column_values_to_follow_rule",
            "matchAtLeastOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
            "matchExactlyOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
            "recommended": "expect_column_values_to_not_match_regex_list",
            "protectAges": "expect_column_values_to_be_between",
            "unique": "expect_column_values_to_be_unique",
        }
        
        #create blank expectation suite
        expectation_suite_name = "Manifest_test_suite"       
        self.suite = self.context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=True
        )
        #print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')        

        #build expectation configurations for each expecation
        for col in self.manifest.columns:

            # remove trailing/leading whitespaces from manifest
            self.manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            rule = self.sg.get_node_validation_rules(col)[0]
            
            args={}
            meta={}

            #update to only do regex match
            if re.match(self.unimplemented_expectations,rule):
                continue

            
            args["column"] = col
            args["result_format"] = "COMPLETE"


            #Validate lift of regices
            if len(self.sg.get_node_validation_rules(col)) > 1: #currently unused
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
                
                '''
                [source_component, source_attribute] = rule.split(" ")[1].split(".")
                [target_component, target_attribute] = rule.split(" ")[2].split(".")
                

                target_IDs=self.get_target_manifests(target_component)
                for target_manifest_ID in target_IDs:
                    entity = syn.get(target_manifest_ID)
                    target_manifest=pd.read_csv(entity.path)
                    if target_attribute in target_manifest.columns:
                        target_column = target_manifest[target_attribute]                       
                        #Add Columns to dict to be added after all manifests are parsed
                      
                
                self.add_expectation(
                    rule=rule,
                    args=args,
                    meta=meta,
                    validation_expectation=validation_expectation,
                )
                '''
            elif rule.startswith("recommended"):
                args["mostly"]=0.0000000001
                args["regex_list"]=['^$']
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column to not be empty. **Markdown** `Supported`",
                    },
                    "validation_rule": rule
                }
            elif rule.startswith("protectAges"):
                args["mostly"]=1.0
                args["min_value"]=6550
                args["max_value"]=32849
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect ages to be between 18 years (6,570 days) and 90 years (32,850 days) of age. **Markdown** `Supported`",
                    },
                    "validation_rule": rule
                }
            elif rule.startswith("unique"):
                args["mostly"]=1.0
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Expect column values to be Unique. **Markdown** `Supported`",
                    },
                    "validation_rule": rule
                }
                                   
            #add expectation for attribute to suite        
            if not rule.startswith("matchAtLeastOne" or "matchExactlyOne"):
                self.add_expectation(
                    rule=rule,
                    args=args,
                    meta=meta,
                    validation_expectation=validation_expectation,
                )
    
        #print(self.context.get_expectation_suite(expectation_suite_name=expectation_suite_name))
        self.context.save_expectation_suite(expectation_suite=self.suite, expectation_suite_name=expectation_suite_name)

        suite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)
        self.context.build_data_docs(resource_identifiers=[suite_identifier])
        ##Webpage DataDocs opened here:
        #self.context.open_data_docs(resource_identifier=suite_identifier) 

    def add_expectation(
        self,
        rule: str,
        args: Dict,
        meta: Dict,
        validation_expectation: Dict,
        ):
        # Create an Expectation
        expectation_configuration = ExpectationConfiguration(
            # Name of expectation type being added
            expectation_type=validation_expectation[rule.split(" ")[0]],

            #add arguments and meta message
            kwargs={**args},
            meta={**meta}
        )
        # Add the Expectation to the suite
        self.suite.add_expectation(expectation_configuration=expectation_configuration)

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
    
    def generate_errors(
        self,
        validation_results: Dict,
        validation_types: Dict,
        errors: List,
        warnings: List
        ):
        type_dict={
            "float64": float,
            "int64": int,
            "str": str,
        }
        for result_dict in validation_results[0]['results']:

            
            indices = []
            values = []

            #print(result_dict)
            #print(result_dict['expectation_config']['expectation_type'])
            pass

            
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
                    for i, item in enumerate(self.manifest[errColumn]):
                        observed_type=result_dict['result']['observed_value']
                        indices.append(i)   if isinstance(item,type_dict[observed_type]) else indices
                        values.append(item) if isinstance(item,type_dict[observed_type]) else values

                #call functions to generate error messages and add to error list
                if validation_types[rule.split(" ")[0]]=='type_validation':
                    for row, value in zip(indices,values):
                        errors.append(
                            GenerateError.generate_type_error(
                                val_rule = rule,
                                row_num = row+2,
                                attribute_name = errColumn,
                                invalid_entry = value,
                            )
                        )                                      
                elif validation_types[rule.split(" ")[0]]=='regex_validation':
                    expression=result_dict['expectation_config']['kwargs']['regex']

                    for row, value in zip(indices,values):   
                        errors.append(
                            GenerateError.generate_regex_error(
                                val_rule= rule,
                                reg_expression = expression,
                                row_num = row+2,
                                module_to_call = 'match',
                                attribute_name = errColumn,
                                invalid_entry = value,
                            )
                        )    
                elif validation_types[rule.split(" ")[0]]=='content_validation':
                    
                    content_errors, content_warnings = GenerateError.generate_content_error(
                                                            val_rule = rule, 
                                                            attribute_name = errColumn,
                                                            row_num = list(np.array(indices)+2),
                                                            error_val = values,  
                                                            sg = self.sg
                                                        )       
                    if content_errors:
                        print(content_errors)
                        errors.append(content_errors)  
                    if content_warnings:
                        print(content_warnings)
                        warnings.append(content_warnings)  
        return errors, warnings
