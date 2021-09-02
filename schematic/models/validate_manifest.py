import json
from jsonschema import Draft7Validator, exceptions, ValidationError
import logging

# import numpy as np
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

logger = logging.getLogger(__name__)


class ValidateManifest(object):
    def __init__(self, errors, manifest, sg, jsonSchema):
        self.errors = errors
        self.manifest = manifest
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
