import logging
import os
import re
from statistics import mode
from tabnanny import check

# allows specifying explicit variable types
from typing import Any, Dict, List, Optional, Text
from urllib import error
from urllib.parse import urlparse
from urllib.request import HTTPDefaultErrorHandler, OpenerDirector, Request, urlopen

import numpy as np
from attr import attr
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.exceptions.exceptions import GreatExpectationsError
from ruamel import yaml

import great_expectations as ge
from schematic.models.validate_attribute import GenerateError
from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.utils.schema_utils import extract_component_validation_rules
from schematic.utils.validate_utils import (
    iterable_to_str_list,
    np_array_to_str_list,
    required_is_only_rule,
    rule_in_rule_list,
)

logger = logging.getLogger(__name__)


# List of modifiers that users can add to a rule, that arent rules themselves.
# as additional modifiers are added will need to update this list

RULE_MODIFIERS = ["error", "warning", "strict", "like", "set", "value"]
VALIDATION_EXPECTATION = {
    "int": "expect_column_values_to_be_in_type_list",
    "float": "expect_column_values_to_be_in_type_list",
    "str": "expect_column_values_to_be_of_type",
    "num": "expect_column_values_to_be_in_type_list",
    "date": "expect_column_values_to_be_dateutil_parseable",
    "recommended": "expect_column_values_to_not_be_null",
    "protectAges": "expect_column_values_to_be_between",
    "unique": "expect_column_values_to_be_unique",
    "inRange": "expect_column_values_to_be_between",
    "IsNA": "expect_column_values_to_match_regex_list",
    # To be implemented rules with possible expectations
    # "list": "expect_column_values_to_not_match_regex_list",
    # "regex": "expect_column_values_to_match_regex",
    # "url": "expect_column_values_to_be_valid_urls",
    # "matchAtLeastOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
    # "matchExactlyOne": "expect_foreign_keys_in_column_a_to_exist_in_column_b",
    # "matchNone": "expect_compound_columns_to_be_unique",
}


class GreatExpectationsHelpers(object):
    """
    Great Expectations helper class

    Provides basic utilities to:
        1) Create GE workflow specific to manifest according to validation rules
        2) Parse results dict to generate appropriate errors
    """

    def __init__(self, dmge, unimplemented_expectations, manifest, manifestPath):
        """
        Purpose:
            Instantiate a great expectations helpers object
        Args:
            dmge:
                DataModelGraphExplorer Object
            unimplemented_expectations:
                dictionary of validation rules that currently do not have expectations developed
            manifest:
                manifest being validated
            manifestPath:
                path to manifest being validated
        """
        self.unimplemented_expectations = unimplemented_expectations
        self.dmge = dmge
        self.manifest = manifest
        self.manifestPath = manifestPath

    def build_context(self):
        """
        Purpose:
            Create a dataContext and datasource and add to object
        Returns:
            saves dataContext and datasource to self
        """
        self.context = ge.get_context()

        # create datasource configuration
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

        # create data context configuration
        data_context_config = DataContextConfig(
            datasources={
                "pandas": DatasourceConfig(
                    class_name="Datasource",
                    execution_engine={"class_name": "PandasExecutionEngine"},
                    data_connectors={
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["default_identifier_name"],
                        }
                    },
                )
            },
            store_backend_defaults=FilesystemStoreBackendDefaults(
                root_directory=os.path.join(os.getcwd(), "great_expectations")
            ),
        )

        # build context and add data source
        self.context = BaseDataContext(project_config=data_context_config)
        # self.context.test_yaml_config(yaml.dump(datasource_config))
        self.context.add_datasource(**datasource_config)

    def add_expectation_suite_if_not_exists(self) -> ExpectationSuite:
        """
        Purpose:
            Add expectation suite if it does not exist
        Input:
        Returns:
            saves expectation suite and identifier to self
        """
        self.expectation_suite_name = "Manifest_test_suite"
        # Get a list of all expectation suites
        suite_names = self.context.list_expectation_suite_names()
        # Get a list of all checkpoints
        all_checkpoints = self.context.list_checkpoints()

        # if the suite exists, delete it
        if self.expectation_suite_name in suite_names:
            self.context.delete_expectation_suite(self.expectation_suite_name)

            # also delete all the checkpoints associated with the suite
            if all_checkpoints:
                for checkpoint_name in all_checkpoints:
                    self.context.delete_checkpoint(checkpoint_name)

        self.suite = self.context.add_expectation_suite(
            expectation_suite_name=self.expectation_suite_name,
        )

        return self.suite

    def build_expectation_suite(
        self,
    ):
        """
        Purpose:
            Construct an expectation suite to validate columns with rules that have expectations
            Add suite to object
        Input:

        Returns:
            saves expectation suite and identifier to self

        """

        # create blank expectation suite
        self.suite = self.add_expectation_suite_if_not_exists()

        # build expectation configurations for each expectation
        for col in self.manifest.columns:
            args = {}
            meta = {}

            # remove trailing/leading whitespaces from manifest
            self.manifest.map(lambda x: x.strip() if isinstance(x, str) else x)

            validation_rules = self.dmge.get_node_validation_rules(
                node_display_name=col
            )

            # check if attribute has any rules associated with it
            if validation_rules:
                # Check if the validation rule applies to this manifest
                if isinstance(validation_rules, dict):
                    validation_rules = extract_component_validation_rules(
                        manifest_component=self.manifest["Component"][0],
                        validation_rules_dict=validation_rules,
                    )
                # iterate through all validation rules for an attribute
                for rule in validation_rules:
                    base_rule = rule.split(" ")[0]

                    # check if rule has an implemented expectation
                    if rule_in_rule_list(
                        rule, self.unimplemented_expectations
                    ) or required_is_only_rule(
                        rule=rule,
                        attribute=col,
                        rule_modifiers=RULE_MODIFIERS,
                        validation_expectation=VALIDATION_EXPECTATION,
                    ):
                        continue

                    args["column"] = col
                    args["result_format"] = "COMPLETE"

                    # Validate num
                    if base_rule == "num":
                        args["mostly"] = 1.0
                        args["type_list"] = ["int", "int64", "float", "float64"]
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be of int or float type. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    # Validate float
                    elif base_rule == "float":
                        args["mostly"] = 1.0
                        args["type_list"] = ["float", "float64"]
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be of float type. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    # Validate int
                    elif base_rule == "int":
                        args["mostly"] = 1.0
                        args["type_list"] = ["int", "int64"]
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be of int type. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    # Validate string
                    elif base_rule == "str":
                        args["mostly"] = 1.0
                        args["type_"] = "str"
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be of string type. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    # Validate date
                    elif base_rule == "date":
                        args["mostly"] = 1.0
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": (
                                    "Expect column values to be parsable by dateutils. "
                                    "**Markdown** `Supported`"
                                ),
                            },
                            "validation_rule": rule,
                        }

                    elif base_rule == ("recommended"):
                        args["mostly"] = 0.0000000001
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column to not be empty. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    elif base_rule == ("protectAges"):
                        # Function to convert to different age limit formats
                        min_age, max_age = self.get_age_limits()

                        args["mostly"] = 1.0
                        args["min_value"] = min_age
                        args["max_value"] = max_age
                        # args['allow_cross_type_comparisons']=True # TODO Can allow after issue #980 is completed
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect ages to be between 18 years (6,570 days) and 90 years (32,850 days) of age. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    elif base_rule == ("unique"):
                        args["mostly"] = 1.0
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be Unique. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    elif base_rule == ("inRange"):
                        args["mostly"] = 1.0
                        args["min_value"] = (
                            float(rule.split(" ")[1])
                            if rule.split(" ")[1].lower() != "none"
                            else None
                        )
                        args["max_value"] = (
                            float(rule.split(" ")[2])
                            if rule.split(" ")[2].lower() != "none"
                            else None
                        )
                        args[
                            "allow_cross_type_comparisons"
                        ] = True  # TODO Should follow up with issue #980
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be within a specified range. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    elif base_rule == ("IsNA"):
                        args["mostly"] = 1.0
                        args["regex_list"] = ["Not Applicable"]
                        meta = {
                            "notes": {
                                "format": "markdown",
                                "content": "Expect column values to be marked Not Applicable. **Markdown** `Supported`",
                            },
                            "validation_rule": rule,
                        }

                    # add expectation for attribute to suite
                    self.add_expectation(
                        rule=rule,
                        args=args,
                        meta=meta,
                        validation_expectation=VALIDATION_EXPECTATION,
                    )

        self.context.update_expectation_suite(
            expectation_suite=self.suite,
        )

        suite_identifier = ExpectationSuiteIdentifier(
            expectation_suite_name=self.expectation_suite_name
        )
        self.context.build_data_docs(resource_identifiers=[suite_identifier])
        ##Webpage DataDocs opened here:
        # self.context.open_data_docs(resource_identifier=suite_identifier)

    def add_expectation(
        self,
        rule: str,
        args: Dict,
        meta: Dict,
        validation_expectation: Dict,
    ):
        """
        Purpose:
            Add individual expectation for a rule to the suite
        Input:
            rule:
                validation rule
            args:
                dict of arguments specifying expectation behavior
            meta:
                dict of additional information for each expectation
            validation_expectation:
                dictionary to map between rules and expectations
        Returns:
            adds expectation to self.suite

        """
        # Create an Expectation
        expectation_configuration = ExpectationConfiguration(
            # Name of expectation type being added
            expectation_type=VALIDATION_EXPECTATION[rule.split(" ")[0]],
            # add arguments and meta message
            kwargs={**args},
            meta={**meta},
        )
        # Add the Expectation to the suite
        self.suite.add_expectation(expectation_configuration=expectation_configuration)

    def build_checkpoint(self):
        """
        Purpose:
            Build checkpoint to validate manifest
        Input:
        Returns:
            adds checkpoint to self
        """
        # create manifest checkpoint
        self.checkpoint_name = "manifest_checkpoint"
        checkpoint_config = {
            "name": self.checkpoint_name,
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "example_datasource",
                        "data_connector_name": "default_runtime_data_connector_name",
                        "data_asset_name": "Manifest",
                    },
                    "expectation_suite_name": self.expectation_suite_name,
                }
            ],
        }

        # self.context.test_yaml_config(yaml.dump(checkpoint_config),return_mode="report_object")
        self.context.add_checkpoint(**checkpoint_config)

    def generate_errors(
        self,
        validation_results: Dict,
        validation_types: Dict,
        errors: List,
        warnings: List,
        dmge: DataModelGraphExplorer,
    ):
        """
        Purpose:
            Parse results dictionary and generate errors for expectations
        Input:
            validation_results:
                dictionary of results for each expectation
            validation_types:
                dict of types of errors to generate for each validation rule
            errors:
                list of errors
            warnings:
                list of warnings
        Returns:
            errors:
                list of errors
            warnings:
                list of warnings
            self.manifest:
                manifest, possibly updated (censored ages)
        """

        type_dict = {
            "float64": float,
            "int64": int,
            "str": str,
        }
        for result_dict in validation_results[0]["results"]:
            indices = []
            values = []

            # if the expectaion failed, get infromation to generate error message
            if not result_dict["success"]:
                errColumn = result_dict["expectation_config"]["kwargs"]["column"]
                rule = result_dict["expectation_config"]["meta"]["validation_rule"]

                if (
                    "exception_info" in result_dict.keys()
                    and result_dict["exception_info"]["exception_message"]
                ):
                    raise GreatExpectationsError(
                        result_dict["exception_info"]["exception_traceback"]
                    )

                # only some expectations explicitly list unexpected values and indices, read or find if not present
                elif "unexpected_index_list" in result_dict["result"]:
                    indices = result_dict["result"]["unexpected_index_list"]
                    values = result_dict["result"]["unexpected_list"]

                # Technically, this shouldn't ever happen, but will keep as a failsafe in case many things go wrong
                # because type validation is column aggregate expectation and not column map expectation when columns are not of object type,
                # indices and values cannot be returned
                else:
                    for i, item in enumerate(self.manifest[errColumn]):
                        observed_type = result_dict["result"]["observed_value"]
                        indices.append(i) if isinstance(
                            item, type_dict[observed_type]
                        ) else indices
                        values.append(item) if isinstance(
                            item, type_dict[observed_type]
                        ) else values

                # call functions to generate error messages and add to error list
                if validation_types[rule.split(" ")[0]]["type"] == "type_validation":
                    for row, value in zip(indices, values):
                        vr_errors, vr_warnings = GenerateError.generate_type_error(
                            val_rule=rule,
                            row_num=str(row + 2),
                            attribute_name=errColumn,
                            invalid_entry=str(value),
                            dmge=dmge,
                        )
                        if vr_errors:
                            errors.append(vr_errors)
                        if vr_warnings:
                            warnings.append(vr_warnings)
                elif validation_types[rule.split(" ")[0]]["type"] == "regex_validation":
                    expression = result_dict["expectation_config"]["kwargs"]["regex"]
                    for row, value in zip(indices, values):
                        vr_errors, vr_warnings = GenerateError.generate_regex_error(
                            val_rule=rule,
                            reg_expression=expression,
                            row_num=str(row + 2),
                            module_to_call="match",
                            attribute_name=errColumn,
                            invalid_entry=value,
                            dmge=dmge,
                        )
                        if vr_errors:
                            errors.append(vr_errors)
                        if vr_warnings:
                            warnings.append(vr_warnings)
                elif (
                    validation_types[rule.split(" ")[0]]["type"] == "content_validation"
                ):
                    vr_errors, vr_warnings = GenerateError.generate_content_error(
                        val_rule=rule,
                        attribute_name=errColumn,
                        row_num=np_array_to_str_list(np.array(indices) + 2),
                        invalid_entry=iterable_to_str_list(values),
                        dmge=self.dmge,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                        if rule.startswith("protectAges"):
                            self.censor_ages(vr_errors, errColumn)

                    if vr_warnings:
                        warnings.append(vr_warnings)
                        if rule.startswith("protectAges"):
                            self.censor_ages(vr_warnings, errColumn)

        return errors, warnings

    def get_age_limits(
        self,
    ):
        """
        Purpose:
            Get boundaries of ages that need to be censored for different age formats
        Input:
        Returns:
            min_age:
                minimum age that will not be censored
            max age:
                maximum age that will not be censored

        """

        min_age = 6550  # days
        max_age = 32849  # days

        return min_age, max_age

    def censor_ages(
        self,
        message: List,
        col: str,
    ):
        """
        Purpose:
            Censor ages in manifest as appropriate
        Input:
            message:
                error or warning message for age validation rule
            col:
                name of column containing ages
        Returns:
            updates self.manifest with censored ages
        TODO: Speed up conversion from str list to int list
        """
        censor_rows = []

        for row in message[0]:
            censor_rows.append(int(row) - 2)

        self.manifest.loc[censor_rows, (col)] = "age censored"

        # update the manifest file, so that ages are censored
        self.manifest.to_csv(
            self.manifestPath.replace(".csv", "_censored.csv"), index=False
        )
        logging.info("Sensitive ages have been censored.")

        return
