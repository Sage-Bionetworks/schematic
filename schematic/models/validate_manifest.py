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
from schematic.models.GE_Helpers import GreatExpectationsHelpers

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier

logger = logging.getLogger(__name__)

class ValidateManifest(object):
    def __init__(self, errors, manifest, manifestPath, sg, jsonSchema):
        self.errors = errors
        self.manifest = manifest
        self.manifestPath = manifestPath
        self.sg = sg
        self.jsonSchema = jsonSchema       

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
        self, manifest: pd.core.frame.DataFrame, sg: SchemaGenerator, restrict_rules: bool
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
            "matchAtLeastOne": "cross_validation",
            "matchExactlyOne": "cross_validation",
            "recommended": "content_validation",
            "protectAges": "content_validation",
            "unique": "content_validation",
            "inRange": "content_validation",
        }

        type_dict={
            "float64": float,
            "int64": int,
            "str": str,
        }

        unimplemented_expectations=[
            "url",
            "list",
            "regex.*",
            "matchAtLeastOne.*",
            "matchExactlyOne.*",
            ]

        in_house_rules = [
            "int",
            "float",
            "num",
            "str",
            "regex.*",
            "url",
            "list",
            "matchAtLeastOne.*",
            "matchExactlyOne.*",
        ]

        # initialize error and warning handling lists.
        errors = []   
        warnings = [] 

        unimplemented_expectations='|'.join(unimplemented_expectations)
        in_house_rules='|'.join(in_house_rules)

        if not restrict_rules:
            #operations necessary to set up and run ge suite validation
            ge_helpers=GreatExpectationsHelpers(
                sg=sg,
                unimplemented_expectations=unimplemented_expectations,
                manifest = manifest,
                manifestPath = self.manifestPath,
                )

            ge_helpers.build_context()
            ge_helpers.build_expectation_suite()
            ge_helpers.build_checkpoint()

        #run GE validation
            results = ge_helpers.context.run_checkpoint(
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
            validation_results = results.list_validation_results()
            

            #parse validation results dict and generate errors
            errors, warnings = ge_helpers.generate_errors(
                errors = errors,
                warnings = warnings,
                validation_results = validation_results,
                validation_types = validation_types,
                )               
        else:             
            logging.info("Great Expetations suite will not be utilized.")  

        for col in manifest.columns:
            # remove trailing/leading whitespaces from manifest
            manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            validation_rules = sg.get_node_validation_rules(col)

            
            # Given a validation rule, run validation. Skip validations already performed by GE
            if bool(validation_rules) and (restrict_rules or re.match(unimplemented_expectations,validation_rules[0])):
                
                if not re.match(in_house_rules,validation_rules[0]):
                    logging.warning(f"Validation rule {validation_rules[0].split(' ')[0]} has not been implemented in house and cannnot be validated without Great Expectations.")
                    continue

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
                        vr_errors, vr_warnings, manifest_col = validation_method(
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
                            second_error, second_warning = validation_method(
                                    self, validation_rules[1], manifest[col]
                            )
                            if second_error:
                                vr_errors.append(
                                    second_error
                                )
                            if second_warning:
                                vr_warnings.append(
                                    second_warning
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
                        vr_errors, vr_warnings, manifest_col = validation_method(
                            self, validation_rules[0], manifest[col]
                        )
                        manifest[col] = manifest_col
                    else:
                        vr_errors, vr_warnings = validation_method(
                            self, validation_rules[0], manifest[col]
                        )
                # Check for validation rule errors and add them to other errors.
                if vr_errors:
                    errors.extend(vr_errors)
                if vr_warnings:
                    warnings.extend(vr_warnings)
        return manifest, errors, warnings

    def validate_manifest_values(self, manifest, jsonSchema):
        
        errors = []
        warnings = []
        annotations = json.loads(manifest.astype('string').to_json(orient="records"))
        for i, annotation in enumerate(annotations):
            v = Draft7Validator(jsonSchema)
            for error in sorted(v.iter_errors(annotation), key=exceptions.relevance):
                errorRow = i + 2
                errorCol = error.path[-1] if len(error.path) > 0 else "Wrong schema"
                errorMsg = error.message[0:500]
                errorVal = error.instance if len(error.path) > 0 else "Wrong schema"

                errors.append([errorRow, errorCol, errorMsg, errorVal])
        return errors, warnings


def validate_all(self, errors, warnings, manifest, manifestPath, sg, jsonSchema, restrict_rules):
    vm = ValidateManifest(errors, manifest, manifestPath, sg, jsonSchema)
    manifest, vmr_errors, vmr_warnings = vm.validate_manifest_rules(manifest, sg, restrict_rules)
    if vmr_errors:
        errors.extend(vmr_errors)
    if vmr_warnings:
        warnings.extend(vmr_warnings)

    vmv_errors, vmv_warnings = vm.validate_manifest_values(manifest, jsonSchema)
    if vmv_errors:
        errors.extend(vmv_errors)
    if vmv_warnings:
        warnings.extend(vmv_warnings)

    return errors, warnings, manifest
