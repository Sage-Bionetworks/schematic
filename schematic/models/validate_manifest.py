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
from schematic.utils.validate_rules_utils import validation_rule_info
from schematic.utils.validate_utils import rule_in_rule_list

logger = logging.getLogger(__name__)

class ValidateManifest(object):
    def __init__(self, errors, manifest, manifestPath, sg, jsonSchema):
        self.errors = errors
        self.manifest = manifest
        self.manifestPath = manifestPath
        self.sg = sg
        self.jsonSchema = jsonSchema       

    def get_multiple_types_error(
        self, validation_rules: list, attribute_name: str, error_type: str
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
        self, manifest: pd.core.frame.DataFrame, sg: SchemaGenerator, restrict_rules: bool, project_scope: List,
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

        validation_types = validation_rule_info()

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


        regex_re=re.compile('regex.*')
        for col in manifest.columns:
            # remove trailing/leading whitespaces from manifest
            manifest.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            validation_rules = sg.get_node_validation_rules(col)

            # Check that attribute rules conform to limits:
            # no more than two rules for an attribute. 
            # As more combinations get added, may want to bring out into its own function / or use validate_rules_utils?
            if len(validation_rules) > 2:
                errors.append(
                    self.get_multiple_types_error(
                        validation_rules, col, error_type="too_many_rules"
                    )
                )

            # Given a validation rule, run validation. Skip validations already performed by GE
            for rule in validation_rules:
                validation_type = rule.split(" ")[0]
                if rule_in_rule_list(rule,unimplemented_expectations) or (rule_in_rule_list(rule,in_house_rules) and restrict_rules):
                    if not rule_in_rule_list(rule,in_house_rules):
                        logging.warning(f"Validation rule {rule.split(' ')[0]} has not been implemented in house and cannnot be validated without Great Expectations.")
                        continue  

                    #Validate for each individual validation rule.
                    validation_method = getattr(
                            ValidateAttribute, validation_types[validation_type]['type']
                        )

                    if validation_type == "list":
                        vr_errors, vr_warnings, manifest_col = validation_method(
                            self, rule, manifest[col]
                        )
                        manifest[col] = manifest_col
                    elif validation_type.lower().startswith("match"):
                        vr_errors, vr_warnings = validation_method(
                            self, rule, manifest[col], project_scope,
                        )
                    else:
                        vr_errors, vr_warnings = validation_method(
                            self, rule, manifest[col]
                        )
                    # Check for validation rule errors and add them to other errors.
                    if vr_errors:
                        errors.extend(vr_errors)
                    if vr_warnings:
                        warnings.extend(vr_warnings)

        return manifest, errors, warnings

    def validate_manifest_values(self, manifest, jsonSchema
    ) -> (List[List[str]], List[List[str]]):
        
        errors = []
        warnings = []
        col_attr = {} # save the mapping between column index and attribute name
        
        # numerical values need to be type string for the jsonValidator
        for col in manifest.select_dtypes(include=[int, np.int64, float, np.float64]).columns:
            manifest[col]=manifest[col].astype('string')
        manifest = manifest.applymap(lambda x: str(x) if isinstance(x, (int, np.int64, float, np.float64)) else x, na_action='ignore')

        annotations = json.loads(manifest.to_json(orient="records"))
        for i, annotation in enumerate(annotations):
            v = Draft7Validator(jsonSchema)
            for error in sorted(v.iter_errors(annotation), key=exceptions.relevance):
                errorRow = i + 2
                errorCol = error.path[-1] if len(error.path) > 0 else "Wrong schema"
                errorColName = error.path[0] if len(error.path) > 0 else "Wrong schema"
                errorMsg = error.message[0:500]
                errorVal = error.instance if len(error.path) > 0 else "Wrong schema"

                errors.append([errorRow, errorCol, errorMsg, errorVal])
                col_attr[errorCol] = errorColName
        if errors: 
            for error in errors: 
                row_num = error[0]
                col_index = error[1]
                attr_name = col_attr[col_index]
                errorMsg = error[2]
                GenerateError.generate_schema_error(row_num = row_num, attribute_name = attr_name, error_msg = errorMsg)

        return errors, warnings


def validate_all(self, errors, warnings, manifest, manifestPath, sg, jsonSchema, restrict_rules, project_scope: List):
    vm = ValidateManifest(errors, manifest, manifestPath, sg, jsonSchema)
    manifest, vmr_errors, vmr_warnings = vm.validate_manifest_rules(manifest, sg, restrict_rules, project_scope)
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
