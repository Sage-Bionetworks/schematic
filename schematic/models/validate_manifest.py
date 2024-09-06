import json
import logging
import os
import re
import sys
from numbers import Number
from statistics import mode
from tabnanny import check
from time import perf_counter

# allows specifying explicit variable types
from typing import Any, Dict, List, Optional, Text
from urllib import error
from urllib.parse import urlparse
from urllib.request import HTTPDefaultErrorHandler, OpenerDirector, Request, urlopen

import numpy as np
import pandas as pd
from jsonschema import Draft7Validator, ValidationError, exceptions

from schematic.models.GE_Helpers import GreatExpectationsHelpers
from schematic.models.validate_attribute import GenerateError, ValidateAttribute
from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.store.synapse import SynapseStorage
from schematic.utils.schema_utils import extract_component_validation_rules
from schematic.utils.validate_rules_utils import validation_rule_info
from schematic.utils.validate_utils import (
    convert_nan_entries_to_empty_strings,
    rule_in_rule_list,
)

logger = logging.getLogger(__name__)


class ValidateManifest(object):
    def __init__(self, errors, manifest, manifestPath, dmge, jsonSchema):
        self.errors = errors
        self.manifest = manifest
        self.manifestPath = manifestPath
        self.dmge = dmge
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
            logger.error(error_str)
            error_message = error_str
            error_val = f"Multiple Rules: too many rules"
        if error_type == "list_not_first":
            error_str = (
                f"For attribute {attribute_name}, the provided validation rules ({validation_rules}) are improperly "
                f"specified. 'list' must be first."
            )
            logger.error(error_str)
            error_message = error_str
            error_val = f"Multiple Rules: list not first"
        return ["NA", error_col, error_message, error_val]

    def check_max_rule_num(
        self,
        validation_rules: list[str],
        col: pd.core.series.Series,
        errors: list[list[str]],
    ) -> list[list[str]]:
        """Check that user isnt applying more rule combinations than allowed. Do not consider certain rules as a part of this rule limit.
        Args:
            validation_rules, list: Validation rules for current manifest column/attribute being evaluated
            col, pd.core.series.Series: the current manifest column being evaluated
            errors, list[list[str]]: list of errors being compiled.
        Returns:
            errors, list[list[str]]: list of errors being compiled, with additional error list being appended if appropriate
        """
        # Check that attribute rules conform to limits:
        # IsNa and required is operate differently than most rules, do not consider it as a rule for evaluating
        # if the number of rule pairs has been exceeded.
        if "IsNa" in validation_rules:
            validation_rules.remove("IsNa")

        if "required" in validation_rules:
            validation_rules.remove("required")

        # no more than two rules for an attribute.
        # As more combinations get added, may want to bring out into its own function / or use validate_rules_utils?
        if len(validation_rules) > 2:
            errors.append(
                self.get_multiple_types_error(
                    validation_rules, col, error_type="too_many_rules"
                )
            )
        return errors

    def validate_manifest_rules(
        self,
        manifest: pd.core.frame.DataFrame,
        dmge: DataModelGraphExplorer,
        restrict_rules: bool,
        project_scope: list[str],
        dataset_scope: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> (pd.core.frame.DataFrame, list[list[str]]):
        """
        Purpose:
            Take validation rules set for a particular attribute
            and validate manifest entries based on these rules.
        Input:
            manifest: pd.core.frame.DataFrame
                imported from models/metadata.py
                contains metadata input from user for each attribute.
            dmge: DataModelGraphExplorer
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

        type_dict = {
            "float64": float,
            "int64": int,
            "str": str,
        }

        unimplemented_expectations = [
            "url",
            "list",
            "regex.*",
            "matchAtLeastOne.*",
            "matchExactlyOne.*",
            "matchNone.*",
            "filenameExists",
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
            "matchNone.*",
            "filenameExists",
        ]

        # initialize error and warning handling lists.
        errors = []
        warnings = []

        if not restrict_rules:
            t_GE = perf_counter()
            # operations necessary to set up and run ge suite validation
            ge_helpers = GreatExpectationsHelpers(
                dmge=dmge,
                unimplemented_expectations=unimplemented_expectations,
                manifest=manifest,
                manifestPath=self.manifestPath,
            )

            ge_helpers.build_context()
            ge_helpers.build_expectation_suite()
            ge_helpers.build_checkpoint()

            try:
                # run GE validation
                results = ge_helpers.context.run_checkpoint(
                    checkpoint_name=ge_helpers.checkpoint_name,
                    batch_request={
                        "runtime_parameters": {"batch_data": manifest},
                        "batch_identifiers": {"default_identifier_name": "manifestID"},
                    },
                    result_format={"result_format": "COMPLETE"},
                )
            finally:
                ge_helpers.context.delete_checkpoint(ge_helpers.checkpoint_name)
                ge_helpers.context.delete_expectation_suite(
                    ge_helpers.expectation_suite_name
                )

            validation_results = results.list_validation_results()

            # parse validation results dict and generate errors
            errors, warnings = ge_helpers.generate_errors(
                errors=errors,
                warnings=warnings,
                validation_results=validation_results,
                validation_types=validation_types,
                dmge=dmge,
            )
            logger.debug(f"GE elapsed time {perf_counter()-t_GE}")
        else:
            logger.info("Great Expetations suite will not be utilized.")

        t_err = perf_counter()
        regex_re = re.compile("regex.*")

        # Instantiate Validate Attribute
        validate_attribute = ValidateAttribute(dmge=dmge)

        for col in manifest.columns:
            # remove trailing/leading whitespaces from manifest
            manifest.map(lambda x: x.strip() if isinstance(x, str) else x)
            validation_rules = dmge.get_node_validation_rules(node_display_name=col)

            # Parse the validation rules
            if validation_rules and isinstance(validation_rules, dict):
                validation_rules = extract_component_validation_rules(
                    manifest_component=manifest["Component"][0],
                    validation_rules_dict=validation_rules,
                )

            # Check for max rule allowance
            errors = self.check_max_rule_num(
                validation_rules=validation_rules, col=col, errors=errors
            )

            # Given a validation rule, run validation. Skip validations already performed by GE
            for rule in validation_rules:
                validation_type = rule.split(" ")[0]
                if rule_in_rule_list(rule, unimplemented_expectations) or (
                    rule_in_rule_list(rule, in_house_rules) and restrict_rules
                ):
                    # Note rules not listed in unimplemented_expectations or inhouse rules will not be run through
                    # the validation steps. IsNA is not a true rule, so it will not have any validation run,
                    # it is handled in validate_attribute
                    if not rule_in_rule_list(rule, in_house_rules):
                        logger.warning(
                            f"Validation rule {rule.split(' ')[0]} has not been implemented in house and cannnot be validated without Great Expectations."
                        )
                        continue

                    t_indiv_rule = perf_counter()
                    # Validate for each individual validation rule.
                    validation_method = getattr(
                        validate_attribute, validation_types[validation_type]["type"]
                    )

                    if validation_type == "list":
                        vr_errors, vr_warnings, manifest_col = validation_method(
                            rule,
                            manifest[col],
                        )
                        manifest[col] = manifest_col
                    elif validation_type.lower().startswith("match"):
                        vr_errors, vr_warnings = validation_method(
                            rule, manifest[col], project_scope, access_token
                        )
                    elif validation_type == "filenameExists":
                        vr_errors, vr_warnings = validation_method(
                            rule,
                            manifest,
                            access_token,
                            dataset_scope,
                            project_scope,
                        )
                    else:
                        vr_errors, vr_warnings = validation_method(
                            rule,
                            manifest[col],
                        )
                    # Check for validation rule errors and add them to other errors.
                    if vr_errors:
                        errors.extend(vr_errors)
                    if vr_warnings:
                        warnings.extend(vr_warnings)
                    logger.debug(
                        f"Rule {rule} elapsed time: {perf_counter()-t_indiv_rule}"
                    )
        logger.debug(f"In House validation elapsed time {perf_counter()-t_err}")
        return manifest, errors, warnings

    def validate_manifest_values(
        self,
        manifest,
        jsonSchema,
        dmge,
    ) -> (List[List[str]], List[List[str]]):
        t_json_schema = perf_counter()

        errors = []
        warnings = []
        col_attr = {}  # save the mapping between column index and attribute name

        manifest = convert_nan_entries_to_empty_strings(manifest=manifest)

        # numerical values need to be type string for the jsonValidator
        for col in manifest.select_dtypes(
            include=[int, np.int64, float, np.float64]
        ).columns:
            manifest[col] = manifest[col].astype("string")
        manifest = manifest.map(
            lambda x: str(x) if isinstance(x, Number) else x, na_action="ignore"
        )

        annotations = json.loads(manifest.to_json(orient="records"))
        for i, annotation in enumerate(annotations):
            v = Draft7Validator(jsonSchema)
            for error in sorted(v.iter_errors(annotation), key=exceptions.relevance):
                errorRow = str(i + 2)
                errorCol = error.path[-1] if len(error.path) > 0 else "Wrong schema"
                errorColName = error.path[0] if len(error.path) > 0 else "Wrong schema"
                errorMsg = error.message[0:500]
                errorVal = error.instance if len(error.path) > 0 else "Wrong schema"

                val_errors, val_warnings = GenerateError.generate_schema_error(
                    row_num=errorRow,
                    attribute_name=errorColName,
                    error_message=errorMsg,
                    invalid_entry=errorVal,
                    dmge=dmge,
                )

                if val_errors:
                    errors.append(val_errors)
                if val_warnings:
                    warnings.append(val_warnings)
        logger.debug(
            f"JSON Schema validation elapsed time {perf_counter()-t_json_schema}"
        )
        return errors, warnings


def validate_all(
    self,
    errors,
    warnings,
    manifest,
    manifestPath,
    dmge,
    jsonSchema,
    restrict_rules,
    project_scope: List,
    dataset_scope: str,
    access_token: str,
):
    # Run Validation Rules
    vm = ValidateManifest(errors, manifest, manifestPath, dmge, jsonSchema)
    manifest, vmr_errors, vmr_warnings = vm.validate_manifest_rules(
        manifest, dmge, restrict_rules, project_scope, dataset_scope, access_token
    )

    if vmr_errors:
        errors.extend(vmr_errors)
    if vmr_warnings:
        warnings.extend(vmr_warnings)

    # Run JSON Schema Validation
    vmv_errors, vmv_warnings = vm.validate_manifest_values(manifest, jsonSchema, dmge)
    if vmv_errors:
        errors.extend(vmv_errors)
    if vmv_warnings:
        warnings.extend(vmv_warnings)

    return errors, warnings, manifest
