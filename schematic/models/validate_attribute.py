import builtins
import logging
import re
from time import perf_counter

# allows specifying explicit variable types
from typing import Any, Optional, Text, Literal, Union
from urllib import error
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
from jsonschema import ValidationError

from schematic.schemas.data_model_graph import DataModelGraphExplorer

from schematic.store.synapse import SynapseStorage
from schematic.utils.validate_rules_utils import validation_rule_info
from schematic.utils.validate_utils import (
    comma_separated_list_regex,
    parse_str_series_to_list,
    np_array_to_str_list,
    iterable_to_str_list,
    rule_in_rule_list,
)

from synapseclient.core.exceptions import SynapseNoCredentialsError

logger = logging.getLogger(__name__)

ScopeTypes = Literal["set", "value"]


class GenerateError:
    def generate_schema_error(
        row_num: str,
        attribute_name: str,
        error_message: str,
        invalid_entry: str,
        dmge: DataModelGraphExplorer,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose: Process error messages generated from schema
        Input:
            - row_num: the row the error occurred on.
            - attribute_name: the attribute the error occurred on.
            - error_message: Error message
            - invalid_entry: The value that caused the error
            - dmge: DataModelGraphExplorer object
        """

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule="schema",
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def generate_list_error(
        list_string: str,
        row_num: str,
        attribute_name: str,
        list_error: str,
        invalid_entry: str,
        dmge: DataModelGraphExplorer,
        val_rule: str,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            If an error is found in the string formatting, detect and record
            an error message.
        Input:
            - list_string: the user input list, that is represented as a string.
            - row_num: the row the error occurred on.
            - attribute_name: the attribute the error occurred on.
            - list_error: the type of list error that occurred.
            - invalid_entry: The value that caused the error
            - dmge: DataModelGraphExplorer object
            - val_rule: validation rule str, defined in the schema.
        Returns
        Errors: list[str] Error details for further storage.
        warnings: list[str] Warning details for further storage.
        """

        if list_error == "not_comma_delimited":
            error_message = (
                f"For attribute {attribute_name} in row {row_num} it does not "
                f"appear as if you provided a comma delimited string. Please check "
                f"your entry ('{list_string}'') and try again."
            )

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def generate_regex_error(
        val_rule: str,
        reg_expression: str,
        row_num: str,
        module_to_call: str,
        attribute_name: str,
        invalid_entry: str,
        dmge: DataModelGraphExplorer,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Generate an logging error as well as a stored error message, when
            a regex error is encountered.
        Input:
            val_rule: str, defined in the schema.
            reg_expression: str, defined in the schema
            row_num: str, row where the error was detected
            module_to_call: re module specified in the schema
            attribute_name: str, attribute being validated
            invalid_entry: value that caused the error
            dmge: DataModelGraphExplorer object
        Returns:
        Errors: list[str] Error details for further storage.
        warnings: list[str] Warning details for further storage.
        """

        error_message = (
            f"For the attribute {attribute_name}, on row {row_num}, the string is not properly formatted. "
            f'It should follow the following re.{module_to_call} pattern "{reg_expression}".'
        )

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def generate_type_error(
        val_rule: str,
        row_num: str,
        attribute_name: str,
        invalid_entry: str,
        dmge: DataModelGraphExplorer,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Generate an logging error as well as a stored error message, when
            a type error is encountered.
        Input:
            val_rule: str, defined in the schema.
            row_num: str, row where the error was detected
            attribute_name: str, attribute being validated
            invalid_entry: str, value that caused the error
            dmge: DataModelGraphExplorer object
        Returns:
        Errors: list[str] Error details for further storage.
        warnings: list[str] Warning details for further storage.
        """

        error_message = (
            f"On row {row_num} the attribute {attribute_name} "
            f"does not contain the proper value type {val_rule}."
        )

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def generate_url_error(
        url: str,
        url_error: str,
        row_num: str,
        attribute_name: str,
        argument: str,
        invalid_entry: str,
        dmge: DataModelGraphExplorer,
        val_rule: str,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Generate an logging error as well as a stored error message, when
            a URL error is encountered.

            Types of errors included:
                - Invalid URL: Refers to a URL that brings up an error when
                    attempted to be accessed such as a HTTPError 404 Webpage Not Found.
                - Argument Error: this refers to a valid URL that does not
                    contain within it the arguments specified by the schema,
                    such as 'protocols.io' or 'dox.doi.org'
                - Random Entry: this refers to an entry try that is not
                    validated to be a URL.
                    e.g. 'lkejrlei', '0', 'not applicable'
        Input:
            url: str, that was input by the user.
            url_error: str, error detected in url_validation()
            row_num: str, row where the error was detected
            attribute_name: str, attribute being validated
            argument: str, argument being validated.
            invalid_entry: str, value that caused the error
            dmge: DataModelGraphExplorer object
            val_rule: validation rule str, defined in the schema.
        Returns:
        Errors: list[str] Error details for further storage.
        warnings: list[str] Warning details for further storage.
        """

        if url_error == "invalid_url":
            error_message = (
                f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                f"conform to the standards of a URL. Please make sure you are entering a real, working URL "
                f"as required by the Schema."
            )
            error_val = invalid_entry
        elif url_error == "arg_error":
            error_message = (
                f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                f"conform to the schema specifications and does not contain the required element: {argument}."
            )
            error_val = f"URL Error: Argument Error"
        elif url_error == "random_entry":
            error_message = (
                f"For the attribute '{attribute_name}', on row {row_num}, the input provided ('{url}'') does not "
                f"look like a URL, please check input and try again."
            )
            error_val = f"URL Error: Random Entry"

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=error_val,
        )

        return error_list, warning_list

    def generate_cross_warning(
        val_rule: str,
        attribute_name: str,
        dmge: DataModelGraphExplorer,
        matching_manifests: list[str] = None,
        manifest_id: Optional[list[str]] = None,
        invalid_entry: Optional[list[str]] = None,
        row_num: Optional[list[str]] = None,
    ) -> tuple[[list[str], list[str]]]:
        """
        Purpose:
            Generate an logging error as well as a stored error message, when
            a cross validation error is encountered.
        Input:
            val_rule: str, defined in the schema.
            attribute_name: str, attribute being validated
            dmge: DataModelGraphExplorer object
            matching_manifests: list of manifests with all values in the target attribute present
            manifest_id: list, synID of the target manifest missing the source value
            invalid_entry: list, value present in source manifest that is missing in the target
            row_num: list, row in source manifest with value missing in target manifests
        Returns:
        Errors: list[str] Error details for further storage.
        warnings: list[str] Warning details for further storage.
        """

        if "matchAtLeast" in val_rule:
            error_message = f"Value(s) {invalid_entry} from row(s) {row_num} of the attribute {attribute_name} in the source manifest are missing."
            error_message += (
                f" Manifest(s) {manifest_id} are missing the value(s)."
                if manifest_id
                else ""
            )

        elif "matchExactly" in val_rule:
            if matching_manifests and matching_manifests != []:
                error_message = f"All values from attribute {attribute_name} in the source manifest are present in {len(matching_manifests)} manifests instead of only 1."
                error_message += f" Manifests {matching_manifests} match the values in the source attribute."

            elif "set" in val_rule:
                error_message = f"No matches for the values from attribute {attribute_name} in the source manifest are present in any other manifests instead of being present in exactly 1. "
            elif "value" in val_rule:
                error_message = f"Value(s) {invalid_entry} from row(s) {row_num} of the attribute {attribute_name} in the source manifest are not present in only one other manifest. "

        elif "matchNone" in val_rule:
            error_message = (
                f"Value(s) {invalid_entry} from row(s) {row_num} for the attribute {attribute_name} "
                f"in the source manifest are not unique."
            )
            error_message += (
                f" Manifest(s) {manifest_id} contain duplicate values."
                if manifest_id
                else ""
            )

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def generate_content_error(
        val_rule: str,
        attribute_name: str,
        dmge: DataModelGraphExplorer,
        row_num=None,
        invalid_entry=None,
    ) -> tuple[list[str], list[str]]:
        """
        Purpose:
            Generate an logging error or warning as well as a stored error/warning message when validating the content of a manifest attribute.

            Types of error/warning included:
                - recommended - Raised when an attribute is empty and recommended but not required.
                - unique - Raised when attribute values are not unique.
                - protectAges - Raised when an attribute contains ages below 18YO or over 90YO that should be censored.
        Input:
                val_rule: str, defined in the schema.
                attribute_name: str, attribute being validated
                dmge: DataModelGraphExplorer object
                row_num: str, row where the error was detected
                invalid_entry: erroneous value(s)
        Returns:
            Errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        """

        error_row = row_num
        error_val = iterable_to_str_list(set(invalid_entry)) if invalid_entry else None

        # log warning or error message
        if val_rule.startswith("recommended"):
            error_message = f"Column {attribute_name} is recommended but empty."
            error_row = None
            error_val = None

        elif val_rule.startswith("unique"):
            error_message = f"Column {attribute_name} has the duplicate value(s) {error_val} in rows: {row_num}."

        elif val_rule.startswith("protectAges"):
            error_message = f"Column {attribute_name} contains ages that should be censored in rows: {row_num}."

        elif val_rule.startswith("inRange"):
            error_message = f"{attribute_name} values in rows {row_num} are out of the specified range."

        elif val_rule.startswith("date"):
            error_message = (
                f"{attribute_name} values in rows {row_num} are not parsable as dates."
            )

        elif val_rule.startswith("IsNA"):
            error_message = f"{attribute_name} values in rows {row_num} are not marked as 'Not Applicable'."

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=error_row,
            error_col=attribute_name,
            error_message=error_message,
            error_val=error_val,
        )

        return error_list, warning_list

    def generate_no_cross_warning(error_col:str, val_rule:str):
        """ Raise a warning if no columns were found in the specified project to validate against, inform user the
        source manifest will be uploaded without running validation. Retain standard warning 
        """
        error_row = None
        error_message = (f"There are no target columns to validate this manifest against for attribute -{error_col}-,"
                        f"and validation rule -{val_rule}-. It is assumed this is the first manifest in a "
                        f"series to be submitted, so validation will pass, for now, and will run again "
                        f"with the next manifest upload.")
        error_val = None

        warning_list = [error_row, error_col, error_message, error_val]
        return warning_list

    def get_message_level(
        dmge: DataModelGraphExplorer,
        error_col: str,
        error_val: Union[str, list[str]],
        val_rule: str,
    ) -> Optional[str]:
        """
        Purpose:
            Determine whether an error or warning message should be logged and displayed

            Message determination hierarchy is as follows:
                1. Schema errors are always logged as errors.
                2. If a message level is specified in the validation rule, it is logged as such.
                3. If the erroneous value is 'not applicable' and the rule is modified by 'IsNA', no message is logged.
                    3a. Messages are never logged specifically for the IsNA rule.
                4. If no level is specified and there is an erroneous value, level is determined by whether or not the attribute is required and if the rule set is modified by the recommended modifier.
                5. If none of the above conditions apply, the default message level for the rule is logged.
        Input:
                dmge: DataModelGraphExplorer object
                error_col: str, attribute being validated
                error_val: erroneous value
                val_rule: str, defined in the schema.
        Returns:
            'error', 'warning' or None
        Raises:
            Logging messagees, either error, warning, or no message
        """
        not_applicable_strings = [
            "not applicable",
        ]

        rule_parts = val_rule.split(" ")
        rule_name = rule_parts[0]
        specified_level = rule_parts[-1].lower()
        specified_level = (
            specified_level if specified_level in ["error", "warning"] else None
        )

        # Get all of the specified validation rules for the attribute
        validation_rule_list = dmge.get_node_validation_rules(
            node_display_name=error_col
        )

        is_schema_error = rule_name == "schema"
        col_is_recommended = rule_name == "recommended"
        col_is_required = dmge.get_node_required(node_display_name=error_col)
        rules_include_na_modifier = rule_in_rule_list("IsNA", validation_rule_list)
        error_val_is_na = (
            error_val.lower() in not_applicable_strings
            if isinstance(error_val, str)
            else False
        )

        if is_schema_error:
            return "error"

        if specified_level:
            return specified_level

        if (
            error_val_is_na and rules_include_na_modifier
        ) or rule_name.lower() == "isna":
            return None

        if not col_is_required:
            return "warning"
        elif col_is_required and col_is_recommended:
            return None

        default_rule_message_level = validation_rule_info()[rule_name][
            "default_message_level"
        ]
        return default_rule_message_level

    def raise_and_store_message(
        dmge: DataModelGraphExplorer,
        val_rule: str,
        error_row: str,
        error_col: str,
        error_message: str,
        error_val: Union[str, list[str]],
    ) -> tuple[list[str], list[str]]:
        """
        Purpose:
            Log and store error messages in a list for further storage.
        Input:
            - dmge: DataModelGraphExplorer object
            - val_rule: str, single validation rule who's error is being logged
            - error_row: str, row where the error was detected
            - error_col: str, attribute being validated
            - error_message: str, error message string
            - error_val: str, erroneous value
        Raises:
            logger.error or logger.warning or no message
        Returns:
            error_list: list of errors
            warning_list: list of warnings
        """

        error_list = []
        warning_list = []

        message_level = GenerateError.get_message_level(
            dmge,
            error_col,
            error_val,
            val_rule,
        )

        if message_level is None:
            return error_list, warning_list

        message_logger = getattr(logger, message_level)
        message_logger(error_message)

        if message_level == "error":
            error_list = [error_row, error_col, error_message, error_val]
        elif message_level == "warning":
            warning_list = [error_row, error_col, error_message, error_val]

        return error_list, warning_list


class ValidateAttribute(object):
    """
    A collection of functions to validate manifest attributes.
        list_validation
        regex_validation
        type_validation
        url_validation
        cross_validation
        get_target_manifests - helper function
    See functions for more details.
    TODO:
        - Add year validator
        - Add string length validator
    """

    def __init__(self, dmge: DataModelGraphExplorer) -> None:
        self.dmge = dmge

    def get_target_manifests(
        self, target_component: str, project_scope: list[str], access_token: str = None
    ):
        t_manifest_search = perf_counter()
        target_manifest_ids = []
        target_dataset_ids = []

        # login
        try:
            synStore = SynapseStorage(
                access_token=access_token, project_scope=project_scope
            )
        except SynapseNoCredentialsError as e:
            raise ValueError(
                "No Synapse credentials were provided. Credentials must be provided to utilize cross-manfiest validation functionality."
            ) from e

        # Get list of all projects user has access to
        projects = synStore.getStorageProjects(project_scope=project_scope)

        for project in projects:
            # get all manifests associated with datasets in the projects
            target_datasets = synStore.getProjectManifests(projectId=project[0])

            # If the manifest includes the target component, include synID in list
            for target_dataset in target_datasets:
                if (
                    target_component == target_dataset[-1][0].replace(" ", "").lower()
                    and target_dataset[1][0] != ""
                ):
                    target_manifest_ids.append(target_dataset[1][0])
                    target_dataset_ids.append(target_dataset[0][0])
        logger.debug(
            f"Cross manifest gathering elapsed time {perf_counter()-t_manifest_search}"
        )
        return synStore, target_manifest_ids, target_dataset_ids

    def list_validation(
        self,
        val_rule: str,
        manifest_col: pd.core.series.Series,
    ) -> tuple[list[list[str]], list[list[str]], pd.core.series.Series]:
        """
        Purpose:
            Determine if values for a particular attribute are comma separated.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given attribute
        Returns:
            - manifest_col: Input values in manifest arere-formatted to a list
            logger.error or logger.warning.
            Errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        """

        # For each 'list' (input as a string with a , delimiter) entered,
        # convert to a real list of strings, with leading and trailing
        # white spaces removed.
        errors = []
        warnings = []
        manifest_col = manifest_col.astype(str)
        csv_re = comma_separated_list_regex()

        rule_parts = val_rule.lower().split(" ")
        if len(rule_parts) > 1:
            list_robustness = rule_parts[1]
        else:
            list_robustness = "strict"

        if list_robustness == "strict":
            # This will capture any if an entry is not formatted properly. Only for strict lists
            for i, list_string in enumerate(manifest_col):
                if not re.fullmatch(csv_re, list_string):
                    list_error = "not_comma_delimited"
                    vr_errors, vr_warnings = GenerateError.generate_list_error(
                        list_string,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        list_error=list_error,
                        invalid_entry=manifest_col[i],
                        dmge=self.dmge,
                        val_rule=val_rule,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)

        # Convert string to list.
        manifest_col = parse_str_series_to_list(manifest_col)

        return errors, warnings, manifest_col

    def regex_validation(
        self,
        val_rule: str,
        manifest_col: pd.core.series.Series,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Check if values for a given manifest attribue conform to the reguar expression,
            provided in val_rule.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given
                attribute in the manifest
            - dmge: DataModelGraphExplorer Object
            Using this module requres validation rules written in the following manner:
                'regex module regular expression'
                - regex: is an exact string specifying that the input is to be validated as a
                regular expression.
                - module: is the name of the module within re to run ie. search.
                - regular_expression: is the regular expression with which to validate
                the user input.
        Returns:
            - This function will return errors when the user input value
            does not match schema specifications.
            logger.error or logger.warning.
            Errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        TODO:
            move validation to convert step.
        """

        reg_exp_rules = val_rule.split(" ")

        try:
            module_to_call = getattr(re, reg_exp_rules[1])
            reg_expression = reg_exp_rules[2]
        except:
            raise ValidationError(
                f"The regex rules were not provided properly for attribute {manifest_col.name}."
                f" They should be provided as follows ['regex', 'module name', 'regular expression']"
            )

        errors = []
        warnings = []

        validation_rules = self.dmge.get_node_validation_rules(
            node_display_name=manifest_col.name
        )
        if validation_rules and "::" in validation_rules[0]:
            validation_rules = validation_rules[0].split("::")
        # Handle case where validating re's within a list.
        if re.search("list", "|".join(validation_rules)):
            if type(manifest_col[0]) == str:
                # Convert string to list.
                manifest_col = parse_str_series_to_list(manifest_col)

            for i, row_values in enumerate(manifest_col):
                for j, re_to_check in enumerate(row_values):
                    re_to_check = str(re_to_check)
                    if not bool(module_to_call(reg_expression, re_to_check)) and bool(
                        re_to_check
                    ):
                        vr_errors, vr_warnings = GenerateError.generate_regex_error(
                            val_rule=val_rule,
                            reg_expression=reg_expression,
                            row_num=str(i + 2),
                            module_to_call=reg_exp_rules[1],
                            attribute_name=manifest_col.name,
                            invalid_entry=manifest_col[i],
                            dmge=self.dmge,
                        )
                        if vr_errors:
                            errors.append(vr_errors)
                        if vr_warnings:
                            warnings.append(vr_warnings)

        # Validating single re's
        else:
            manifest_col = manifest_col.astype(str)
            for i, re_to_check in enumerate(manifest_col):
                if not bool(module_to_call(reg_expression, re_to_check)) and bool(
                    re_to_check
                ):
                    vr_errors, vr_warnings = GenerateError.generate_regex_error(
                        val_rule=val_rule,
                        reg_expression=reg_expression,
                        row_num=str(i + 2),
                        module_to_call=reg_exp_rules[1],
                        attribute_name=manifest_col.name,
                        invalid_entry=manifest_col[i],
                        dmge=self.dmge,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)

        return errors, warnings

    def type_validation(
        self,
        val_rule: str,
        manifest_col: pd.core.series.Series,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Check if values for a given manifest attribue are the same type
            specified in val_rule.
        Input:
            - val_rule: str, Validation rule, specifying input type, either
                'float', 'int', 'num', 'str'
            - manifest_col: pd.core.series.Series, column for a given
                attribute in the manifest
        Returns:
            -This function will return errors when the user input value
            does not match schema specifications.
            logger.error or logger.warning.
            Errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        TODO:
            Convert all inputs to .lower() just to prevent any entry errors.
        """
        specified_type = {
            "num": (int, np.int64, float),
            "int": (int, np.int64),
            "float": (float),
            "str": (str),
        }

        errors = []
        warnings = []
        # num indicates either a float or int.
        if val_rule == "num":
            for i, value in enumerate(manifest_col):
                if bool(value) and not isinstance(value, specified_type[val_rule]):
                    vr_errors, vr_warnings = GenerateError.generate_type_error(
                        val_rule=val_rule,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        invalid_entry=str(manifest_col[i]),
                        dmge=self.dmge,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)
        elif val_rule in ["int", "float", "str"]:
            for i, value in enumerate(manifest_col):
                if bool(value) and not isinstance(value, specified_type[val_rule]):
                    vr_errors, vr_warnings = GenerateError.generate_type_error(
                        val_rule=val_rule,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        invalid_entry=str(manifest_col[i]),
                        dmge=self.dmge,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)
        return errors, warnings

    def url_validation(
        self,
        val_rule: str,
        manifest_col: str,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Validate URL's submitted for a particular attribute in a manifest.
            Determine if the URL is valid and contains attributes specified in the
            schema.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given
                attribute in the manifest
        Output:
            This function will return errors when the user input value
            does not match schema specifications.
        """

        url_args = val_rule.split(" ")[1:]
        errors = []
        warnings = []

        for i, url in enumerate(manifest_col):
            # Check if a random phrase, string or number was added and
            # log the appropriate error.
            if not isinstance(url, str) or not (
                urlparse(url).scheme
                + urlparse(url).netloc
                + urlparse(url).params
                + urlparse(url).query
                + urlparse(url).fragment
            ):
                #
                url_error = "random_entry"
                valid_url = False
                vr_errors, vr_warnings = GenerateError.generate_url_error(
                    url,
                    url_error=url_error,
                    row_num=str(i + 2),
                    attribute_name=manifest_col.name,
                    argument=url_args,
                    invalid_entry=manifest_col[i],
                    dmge=self.dmge,
                    val_rule=val_rule,
                )
                if vr_errors:
                    errors.append(vr_errors)
                if vr_warnings:
                    warnings.append(vr_warnings)
            else:
                # add scheme to the URL if not currently added.
                if not urlparse(url).scheme:
                    url = "http://" + url
                try:
                    # Check that the URL points to a working webpage
                    # if not log the appropriate error.
                    request = Request(url)
                    response = urlopen(request)
                    valid_url = True
                    response_code = response.getcode()
                except:
                    valid_url = False
                    url_error = "invalid_url"
                    vr_errors, vr_warnings = GenerateError.generate_url_error(
                        url,
                        url_error=url_error,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        argument=url_args,
                        invalid_entry=manifest_col[i],
                        dmge=self.dmge,
                        val_rule=val_rule,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)
                if valid_url == True:
                    # If the URL works, check to see if it contains the proper arguments
                    # as specified in the schema.
                    for arg in url_args:
                        if arg not in url:
                            url_error = "arg_error"
                            vr_errors, vr_warnings = GenerateError.generate_url_error(
                                url,
                                url_error=url_error,
                                row_num=str(i + 2),
                                attribute_name=manifest_col.name,
                                argument=arg,
                                invalid_entry=manifest_col[i],
                                dmge=self.dmge,
                                val_rule=val_rule,
                            )
                            if vr_errors:
                                errors.append(vr_errors)
                            if vr_warnings:
                                warnings.append(vr_warnings)
        return errors, warnings

    def _parse_validation_log(
        self, validation_log: dict[str, pd.core.series.Series]
    ) -> tuple[[list[str], list[str], list[str]]]:
        """Parse validation log, so values can be used to raise warnings/errors
        Args:
            validation_log, dict[str, pd.core.series.Series]:
        Returns:
            invalid_rows, list: invalid rows recorded in the validation log
            invalid_entry, list: invalid values recorded in the validation log
            manifest_ids, list:
        """
        # Initialize parameters
        validation_rows, validation_values = [], []

        manifest_entries = list(validation_log.values())
        manifest_ids = list(validation_log.keys())
        for entry in manifest_entries:
            # Increment pandas row values by 2 so they will match manifest sheet row values.
            validation_rows.append(entry.index[0] + 2)
            validation_values.append(entry.values[0])

        # Parse invalid_rows and invalid_values so they can be used to raise warnings/errors
        invalid_rows = iterable_to_str_list(set(validation_rows))
        invalid_entries = iterable_to_str_list(set(validation_values))

        return invalid_rows, invalid_entries, manifest_ids

    def _merge_format_invalid_rows_values(
        self, series_1: pd.core.series.Series, series_2: pd.core.series.Series
    ) -> tuple[[list[str], list[str]]]:
        """Merge two series to identify gather all invalid values, and parse out invalid rows and entries
        Args:
            series_1, pd.core.series.Series: first set of invalid values to extract
            series_2, pd.core.series.Series: second set of invalid values to extract
        Returns:
            invalid_rows, list: invalid rows taken from both series
            invalid_entry, list: invalid values taken from both series
        """
        # Merge series to get invalid values and rows
        invalid_values = pd.merge(series_1, series_2, how="outer")
        # Increment pandas row values by 2 so they will match manifest sheet row values.
        invalid_rows = (
            pd.merge(
                series_1,
                series_2,
                how="outer",
                left_index=True,
                right_index=True,
            ).index.to_numpy()
            + 2
        )
        # Parse invalid_rows and invalid_values so they can be used to raise warnings/errors
        invalid_rows = np_array_to_str_list(invalid_rows)
        invalid_entry = iterable_to_str_list(invalid_values.squeeze())
        return invalid_rows, invalid_entry

    def _format_invalid_row_values(
        self, invalid_values: dict[str, pd.core.series.Series]
    ) -> tuple[[list[str], list[str]]]:
        """Parse invalid_values dictionary, to extract invalid_rows and invalid_entry to be used later
        to raise warnings or errors.
        Args:
            invalid_values, dict[str, pd.core.series.Series]:
        Returns:
            invalid_rows, list: invalid rows recorded in invalid_values
            invalid_entry, list: invalid values recorded in invalid_values
        """
        # Increment pandas row values by 2 so they will match manifest sheet row values.
        invalid_rows = invalid_values.index.to_numpy() + 2

        # Parse invalid_rows and invalid_values so they can be used to raise warnings/errors
        invalid_rows = np_array_to_str_list(invalid_rows)
        invalid_entry = iterable_to_str_list(invalid_values)
        return invalid_rows, invalid_entry

    def _gather_set_warnings_errors(
        self,
        val_rule: str,
        source_attribute: str,
        set_validation_store: tuple[
            dict[str, pd.core.series.Series],
            list[str],
            dict[str, pd.core.series.Series],
        ],
    ) -> tuple[[list[str], list[str]]]:
        """Based on the cross manifest validation rule, and in set rule scope, pass variables to
        _get_cross_errors_warnings
            to log appropriate error or warning.
        Args:
            val_rule, str: Validation Rule
            source_attribute, str: Source manifest column name
            set_validation_store, tuple[dict[str, pd.core.series.Series], list[string],
            dict[str, pd.core.series.Series]]:
                contains the missing_manifest_log, present_manifest_log, and repeat_manifest_log
            dmge: DataModelGraphExplorer Object.

        Returns:
            errors, list[str]: list of errors to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
            warnings, list[str]: list of warnings to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
        """
        errors, warnings = [], []

        (
            missing_manifest_log,
            present_manifest_log,
            repeat_manifest_log,
        ) = set_validation_store

        # Process error handling by rule
        if "matchAtLeastOne" in val_rule and len(present_manifest_log) < 1:
            (
                invalid_rows,
                invalid_entries,
                manifest_ids,
            ) = self._parse_validation_log(validation_log=missing_manifest_log)
            errors, warnings = self._get_cross_errors_warnings(
                val_rule=val_rule,
                row_num=invalid_rows,
                attribute_name=source_attribute,
                invalid_entry=invalid_entries,
                manifest_id=manifest_ids,
            )

        elif "matchExactlyOne" in val_rule and len(present_manifest_log) != 1:
            errors, warnings = self._get_cross_errors_warnings(
                val_rule=val_rule,
                attribute_name=source_attribute,
                matching_manifests=present_manifest_log,
            )

        elif "matchNone" in val_rule and repeat_manifest_log:
            (
                invalid_rows,
                invalid_entries,
                manifest_ids,
            ) = self._parse_validation_log(validation_log=repeat_manifest_log)
            errors, warnings = self._get_cross_errors_warnings(
                val_rule=val_rule,
                row_num=invalid_rows,
                attribute_name=source_attribute,
                invalid_entry=invalid_entries,
                manifest_id=manifest_ids,
            )

        return errors, warnings

    def _get_cross_errors_warnings(
        self,
        val_rule: str,
        attribute_name: str,
        row_num: Optional[list[str]] = None,
        matching_manifests: Optional[list[str]] = None,
        manifest_id: Optional[list[str]] = None,
        invalid_entry: Optional[list[str]] = None,
    ) -> tuple[[list[str], list[str]]]:
        """Helper to call GenerateError.generate_cross_warning in a consistent way, gather warnings and errors.
        Args:
            val_rule, str: Validation Rule
            attribute_name, str: source attribute name
            row_num, list: default=None, list of rows in the source manifest where invalid values were located
            matching_manifests, list: default=[], ist of manifests with all values in the target attribute present
            manifest_id, list: default=None, list of manifests where invalid values were located.
            invalid_entry, list: default=None, list of entries in the source manifest where invalid values were located.
        Returns:
            errors, list[str]: list of errors to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
            warnings, list[str]: list of warnings to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
        """

        errors, warnings = [], []
        vr_errors, vr_warnings = GenerateError.generate_cross_warning(
            val_rule=val_rule,
            row_num=row_num,
            attribute_name=attribute_name,
            matching_manifests=matching_manifests,
            invalid_entry=invalid_entry,
            manifest_id=manifest_id,
            dmge=self.dmge,
        )
        if vr_errors:
            errors.append(vr_errors)
        if vr_warnings:
            warnings.append(vr_warnings)
        return errors, warnings

    def _gather_value_warnings_errors(
        self,
        val_rule: str,
        source_attribute: str,
        value_validation_store: tuple[
            dict[str, pd.core.series.Series],
            dict[str, pd.core.series.Series],
            dict[str, pd.core.series.Series],
        ],
    ) -> tuple[[list[str], list[str]]]:
        """For value rule scope, find invalid rows and entries, and generate appropriate errors and warnings
        Args:
            val_rule, str: Validation rule
            source_attribute, str: source manifest column name
            value_validation_store, tuple(dict[str, pd.core.series.Series], dict[str, pd.core.series.Series],
                dict[str, pd.core.series.Series]):
                contains missing_values, duplicated_values, and repeat values
        Returns:
            errors, list[str]: list of errors to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
            warnings, list[str]: list of warnings to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
        """
        # Initialize with empty lists
        errors, warnings = [], []
        invalid_rows, invalid_entry = [], []

        # Unpack the value_validation_store
        (missing_values, duplicated_values, repeat_values) = value_validation_store

        # Determine invalid rows and entries based on the rule type
        if "matchAtLeastOne" in val_rule and not missing_values.empty:
            invalid_rows, invalid_entry = self._format_invalid_row_values(
                missing_values
            )

        elif "matchExactlyOne" in val_rule and (
            duplicated_values.any() or missing_values.any()
        ):
            (
                invalid_rows,
                invalid_entry,
            ) = self._merge_format_invalid_rows_values(
                duplicated_values, missing_values
            )

        elif "matchNone" in val_rule and repeat_values.any():
            invalid_rows, invalid_entry = self._format_invalid_row_values(repeat_values)

        # If invalid rows/entries found, raise warning/error
        if invalid_rows and invalid_entry:
            errors, warnings = self._get_cross_errors_warnings(
                val_rule=val_rule,
                row_num=invalid_rows,
                attribute_name=source_attribute,
                invalid_entry=invalid_entry,
            )
        return errors, warnings

    def _run_validation_across_targets_set(
        self,
        val_rule: str,
        column_names: dict[str, str],
        manifest_col: pd.core.series.Series,
        target_attribute: str,
        target_manifest: pd.core.series.Series,
        target_manifest_id: str,
        missing_manifest_log: dict[str, pd.core.series.Series],
        present_manifest_log: dict[str, pd.core.series.Series],
        repeat_manifest_log: dict[str, pd.core.series.Series],
    ) -> tuple[tuple[dict[str, pd.core.series.Series],
        dict[str, pd.core.series.Series],
        dict[str, pd.core.series.Series],
        bool]

    ]:
        """For set rule scope, go through the given target column and look
        Args:
            val_rule, str: Validation rule
            column_names, dict[str,str]: {stripped_col_name:original_column_name}
            target_column, pd.core.series.Series: Empty target_column to fill out in this function
            manifest_col, pd.core.series.Series: Source manifest column
            target_attribute, str: current target attribute
            target_column, pd.core.series.Series: Current target column
            target_manifest, pd.core.series.Series: Current target manifest
            target_manifest_id, str: Current target manifest Synapse ID
            missing_manifest_log, dict[str, pd.core.series.Series]:
                Log of manifests with missing values, {synapse_id: index,missing value}, updated.
            present_manifest_log, dict[str, pd.core.series.Series]
                Log of present manifests, {synapse_id: index,present value}, updated.
            repeat_manifest_log, dict[str, pd.core.series.Series]
                Log of manifests with repeat values, {synapse_id: index,repeat value}, updated.

        Returns:
            tuple(
            missing_manifest_log, dict[str, pd.core.series.Series]:
                Log of manifests with missing values, {synapse_id: index,missing value}, updated.
            present_manifest_log, dict[str, pd.core.series.Series]
                Log of present manifests, {synapse_id: index,present value}, updated.
            repeat_manifest_log, dict[str, pd.core.series.Series]
                Log of manifests with repeat values, {synapse_id: index,repeat value}, updated.)
            target_attribute_in_manifest, bool: True if the target attribute is in the current manifest.
        """
        target_attribute_in_manifest = False
        # If the manifest has the target attribute for the component do the cross validation
        if target_attribute in column_names:
            target_attribute_in_manifest = True
            target_column = target_manifest[column_names[target_attribute]]

            # Do the validation on both columns
            if "matchNone" in val_rule:
                # Look for repeats between the source manifest and target_column, if there are
                # repeats log the repeat value and manifest
                repeat_values = manifest_col[manifest_col.isin(target_column)]

                if repeat_values.any():
                    repeat_manifest_log[target_manifest_id] = repeat_values
            else:
                # Determine elements in manifest column that are missing from the target column
                missing_values = manifest_col[~manifest_col.isin(target_column)]

                if missing_values.empty:
                    # If there are no missing values in the target column, log this manifest as
                    # one where all items are present
                    present_manifest_log.append(target_manifest_id)
                else:
                    # If there are missing values in the target manifest, log the manifest
                    # and the missing values.
                    missing_manifest_log[target_manifest_id] = missing_values

        return (missing_manifest_log, present_manifest_log, repeat_manifest_log), target_attribute_in_manifest

    def _gather_target_columns_value(
        self,
        column_names: dict[str, str],
        target_attribute: str,
        concatenated_target_column: pd.core.series.Series,
        target_manifest: pd.core.series.Series,
    ) -> pd.core.series.Series:
        """A helper function for creating a concatenating all target attribute columns across all target manifest. This function checks if the
        target attribute is in the current target manifest. If it is, and is the first manifest with this column, start recording it, if it has
        already been recorded from another manifest concatenate the new column to the concatenated_target_column series.
        Args:
            column_names, dict: {stripped_col_name:original_column_name}
            target_attribute, str: current target attribute
            concatenated_target_column, pd.core.series.Series: target column in the process of being built, possibly
                passed through this function multiple times based on the number of manifests
            target_manifest, pd.core.series.Series: current target manifest
        Returns:
            concatenated_target_column, pd.core.series.Series: All target columns concatenated into a single column
        """
        # Check if the target_attribute is in the current target manifest.
        if target_attribute in column_names:
            # If it is, make sure the column names match the original column names
            target_manifest.rename(
                columns={column_names[target_attribute]: target_attribute},
                inplace=True,
            )
            # If matches with other columns have already been found, concatenate current target attribute column to
            # the series
            if concatenated_target_column.any():
                concatenated_target_column = pd.concat(
                    objs=[
                        concatenated_target_column,
                        target_manifest[target_attribute],
                    ],
                    join="outer",
                    ignore_index=True,
                )
            else:
                # Otherwise, start recording the target_attribute column
                concatenated_target_column = target_manifest[target_attribute]

            concatenated_target_column = concatenated_target_column.astype("object")

        return concatenated_target_column

    def _run_validation_across_targets_value(
        self,
        manifest_col: pd.core.series.Series,
        concatenated_target_column: pd.core.series.Series,
    ) -> tuple[[pd.core.series.Series, pd.core.series.Series, pd.core.series.Series]]:
        """Get missing values, duplicated values and repeat values assesed comapring the source manifest to all
            the values in all target columns.
        Args:
            manifest_col, pd.core.series.Series: Current source manifest column
            concatenated_target_column, pd.core.series.Series: All target columns concatenated into a single column
        Returns:
            missing_values, pd.core.series.Series: values that are present in the source manifest, but not present
                in the target manifest
            duplicated_values, pd.core.series.Series: values that duplicated in the concatenated target column, and
                also present in the source manifest column
            repeat_values, pd.core.series.Series: values that are repeated between the manifest column and
                concatenated target column
        """
        # Find values that are present in the source manifest, but not present in the target manifest
        missing_values = manifest_col[~manifest_col.isin(concatenated_target_column)]

        # Find values that duplicated in the concatenated target column, and also present in the source manifest column
        duplicated_values = manifest_col[
            manifest_col.isin(
                concatenated_target_column[concatenated_target_column.duplicated()]
            )
        ]

        # Find values that are repeated between the manifest column and concatenated target column
        repeat_values = manifest_col[manifest_col.isin(concatenated_target_column)]

        return missing_values, duplicated_values, repeat_values

    def _get_column_names(
        self, target_manifest: pd.core.series.Series
    ) -> dict[str, str]:
        """Convert manifest column names into validation rule input format
        Args:
            target_manifest, pd.core.series.Series: Current target manifest
        Returns:
            column_names, dict[str,str]: {stripped_col_name:original_column_name}
        """
        column_names = {}
        for original_column_name in target_manifest.columns:
            stripped_col_name = original_column_name.replace(" ", "").lower()
            column_names[stripped_col_name] = original_column_name
        return column_names

    def _get_rule_scope(self, val_rule: str) -> ScopeTypes:
        """Parse scope from validation rule
        Args:
            val_rule, str: Validation Rule
        Returns:
            scope, ScopeTypes: The scope of the rule, taken from validation rule
        """
        scope = val_rule.lower().split(" ")[2]
        return scope

    def _run_validation_across_target_manifests(
        self,
        project_scope: Optional[list[str]],
        rule_scope: ScopeTypes,
        access_token: str,
        val_rule: str,
        manifest_col: pd.core.series.Series,
        target_column: pd.core.series.Series,
    ) -> tuple[[float, list]]:
        """Run cross manifest validation from a source manifest, across all relevant target manifests,
            based on scope. Output start time and validation outputs..
        Args:
            project_scope, Optional[list]: Projects to limit the scope of cross manifest validation to.
            rule_scope, ScopeTypes: The scope of the rule, taken from validation rule
            access_token, str: Asset Store access token
            val_rule, str: Validation rule.
            manifest_col, pd.core.series.Series: Source manifest column for a given source component
            target_column, pd.core.series.Series: Empty target_column to fill out in this function
        Returns:
            start_time, float: start time in fractional seconds
            validation_output, Union[
                tuple[dict[str, pd.core.series.Series], list[str], dict[str, pd.core.series.Series]],
                tuple[dict[str, pd.core.series.Series], dict[str, pd.core.series.Series],
                    dict[str, pd.core.series.Series]]:
                    validation outputs, exact types depend on scope,
        """
        # Initialize variables
        present_manifest_log = []
        duplicated_values = {}
        missing_values = {}
        repeat_values = {}
        missing_manifest_log = {}
        repeat_manifest_log = {}

        target_attribute_in_manifest=False

        # Set relevant parameters
        [target_component, target_attribute] = val_rule.lower().split(" ")[1].split(".")
        target_column.name = target_attribute

        # Get IDs of manifests with target component
        (
            synStore,
            target_manifest_ids,
            target_dataset_ids,
        ) = self.get_target_manifests(target_component, project_scope, access_token)

        # Start timer
        start_time = perf_counter()

        # For each target manifest, gather target manifest column and compare to the source manifest column
        # Save relevant data as appropriate for the given scope
        for target_manifest_id, target_dataset_id in zip(
            target_manifest_ids, target_dataset_ids
        ):
            # Pull manifest from Synapse
            entity = synStore.getDatasetManifest(
                datasetId=target_dataset_id, downloadFile=True
            )
            # Load manifest
            target_manifest = pd.read_csv(entity.path)

            # Get manifest column names
            column_names = self._get_column_names(target_manifest=target_manifest)

            # Read each target manifest and run validation of current manifest column (set) against each
            # manifest individually, gather results
            if "set" in rule_scope:
                (
                    (missing_manifest_log,
                    present_manifest_log,
                    repeat_manifest_log,),
                    target_attribute_in_manifest,
                ) = self._run_validation_across_targets_set(
                    val_rule=val_rule,
                    column_names=column_names,
                    manifest_col=manifest_col,
                    target_attribute=target_attribute,
                    target_manifest=target_manifest,
                    target_manifest_id=target_manifest_id,
                    missing_manifest_log=missing_manifest_log,
                    present_manifest_log=present_manifest_log,
                    repeat_manifest_log=repeat_manifest_log,
                )
            # Concatenate target manifest columns, in a subsequent step will run cross manifest validation from
            # the current manifest
            # column values against the concatenated target column
            if "value" in rule_scope:
                target_column = self._gather_target_columns_value(
                    column_names=column_names,
                    target_attribute=target_attribute,
                    concatenated_target_column=target_column,
                    target_manifest=target_manifest,
                )
                if target_column.any():
                    target_attribute_in_manifest=True
                    
        if not target_attribute_in_manifest:
            return (start_time, target_attribute_in_manifest)
        else:
            # Store outputs according to the scope for which they are used.
            if "set" in rule_scope:
                validation_store = (
                    missing_manifest_log,
                    present_manifest_log,
                    repeat_manifest_log,
                )

            elif "value" in rule_scope:
                # From the concatenated target column, for value scope, run validation
                (
                    missing_values,
                    duplicated_values,
                    repeat_values,
                ) = self._run_validation_across_targets_value(
                    manifest_col=manifest_col,
                    concatenated_target_column=target_column,
                )
                validation_store = (missing_values, duplicated_values, repeat_values)
            return (start_time, validation_store)

    def cross_validation(
        self,
        val_rule: str,
        manifest_col: pd.core.series.Series,
        project_scope: Optional[list[str]],
        access_token: str,
    ) -> list[list[str]]:
        """
        Purpose:
            Do cross validation between the current manifest and all other manifests in a given asset view (limited
                by project scope, if provided).
        Args:
            val_rule, str: Validation rule
            manifest_col, pd.core.series.Series: column for a given
                attribute in the manifest
            project_scope, Optional[list] = None: Projects to limit the scope of cross manifest validation to.
            dmge: DataModelGraphExplorer Object
            access_token, str: Asset Store access token
        Returns:
            errors, warnings, list[list[str]]: raise warnings and errors as appropriate if values in current manifest do
            no pass relevant cross mannifest validation across the target manifest(s)
        """
        # Initialize target_column
        target_column = pd.Series(dtype=object)

        # Get the rule_scope from the validation rule
        rule_scope = self._get_rule_scope(val_rule)

        # Run validation from source to target manifest(s), gather outputs
        (
            start_time,
            validation_output,
        ) = self._run_validation_across_target_manifests(
            project_scope=project_scope,
            rule_scope=rule_scope,
            access_token=access_token,
            val_rule=val_rule,
            manifest_col=manifest_col,
            target_column=target_column,
        )

        # If there are no target_columns to validate against, assume this is the first manifest being submitted and
        # allow users to just submit.
        if isinstance(validation_output, bool) and not validation_output:
            errors = []
            warnings = GenerateError.generate_no_cross_warning(error_col=manifest_col.name, val_rule=val_rule)

        elif isinstance(validation_output, tuple):
            # Raise warnings/errors based on validation output and rule_scope.
            if "set" in rule_scope:
                errors, warnings = self._gather_set_warnings_errors(
                    val_rule=val_rule,
                    source_attribute=manifest_col.name,
                    set_validation_store=validation_output,
                )
            elif "value" in rule_scope:
                errors, warnings = self._gather_value_warnings_errors(
                    val_rule=val_rule,
                    source_attribute=manifest_col.name,
                    value_validation_store=validation_output,
                )

        logger.debug(f"cross manifest validation time {perf_counter()-start_time}")

        return errors, warnings
