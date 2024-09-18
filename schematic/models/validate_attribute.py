import logging
import re
from copy import deepcopy
from time import perf_counter

# allows specifying explicit variable types
from typing import Any, Literal, Optional, Union
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from jsonschema import ValidationError
from synapseclient import File
from synapseclient.core.exceptions import SynapseNoCredentialsError

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.store.synapse import SynapseStorage
from schematic.utils.validate_rules_utils import validation_rule_info
from schematic.utils.validate_utils import (
    comma_separated_list_regex,
    get_list_robustness,
    iterable_to_str_list,
    np_array_to_str_list,
    parse_str_series_to_list,
    rule_in_rule_list,
)

logger = logging.getLogger(__name__)

MessageLevelType = Literal["warning", "error"]
ScopeTypes = Literal["set", "value"]


class GenerateError:
    def generate_schema_error(
        row_num: str,
        attribute_name: str,
        error_message: str,
        invalid_entry: Any,
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
            message_level="error",
        )

        return error_list, warning_list

    def generate_list_error(
        list_string: str,
        row_num: str,
        attribute_name: str,
        list_error: Literal["not_comma_delimited", "not_a_string"],
        invalid_entry: Any,
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
        elif list_error == "not_a_string":
            error_message = (
                f"For attribute {attribute_name} in row {row_num} it does not "
                f"appear as if you provided a string. Please check "
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
        invalid_entry: Any,
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
        invalid_entry: Any,
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

        base_rule = val_rule.split(" ")[0]

        error_message = (
            f"On row {row_num} the attribute {attribute_name} "
            f"does not contain the proper value type {base_rule}."
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
        invalid_entry: Any,
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
        elif url_error == "arg_error":
            error_message = (
                f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                f"conform to the schema specifications and does not contain the required element: {argument}."
            )
        elif url_error == "random_entry":
            error_message = (
                f"For the attribute '{attribute_name}', on row {row_num}, the input provided ('{url}'') does not "
                f"look like a URL, please check input and try again."
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

    def generate_cross_warning(
        val_rule: str,
        attribute_name: str,
        dmge: DataModelGraphExplorer,
        matching_manifests: list[str] = None,
        manifest_id: Optional[list[str]] = None,
        invalid_entry: Union[str, list[str]] = "No Invalid Entry Recorded",
        row_num: Optional[list[str]] = None,
    ) -> tuple[list[list[str]], list[list[str]]]:
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

        # log warning or error message
        if val_rule.startswith("recommended"):
            error_message = f"Column {attribute_name} is recommended but empty."
            error_row = None
            invalid_entry = None

        if val_rule.startswith("unique"):
            invalid_entry = (
                iterable_to_str_list(set(invalid_entry)) if invalid_entry else None
            )
            error_message = f"Column {attribute_name} has the duplicate value(s) {invalid_entry} in rows: {row_num}."

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
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def generate_no_cross_warning(
        dmge: DataModelGraphExplorer, attribute_name: str, val_rule: str
    ) -> list[list[str]]:
        """Raise a warning if no columns were found in the specified project to validate against, inform user the
        source manifest will be uploaded without running validation. Retain standard warning
        Args:
            dmge: DataModelGraphExplorer object
            attribute_name, str: str, attribute being validated
            val_rule: str, defined in the schema.
        Returns:
            warnings: list[str] Warning details for further storage.
        """
        error_message = (
            "Cross Manifest Validation Warning: There are no target columns to validate "
            f"this manifest against for attribute: {attribute_name}, "
            f"and validation rule: {val_rule}. It is assumed this is the first manifest in a "
            "series to be submitted, so validation will pass, for now, and will run again "
            "when there are manifests uploaded to validate against."
        )

        _, warnings = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=None,
            error_col=attribute_name,
            error_message=error_message,
            error_val=None,
            message_level="warning",
        )

        return [warnings]

    def generate_no_value_in_manifest_error(
        dmge: DataModelGraphExplorer, attribute_name: str, val_rule: str
    ) -> tuple[list[str], list[str]]:
        """Raise a warning or error based on the messaging level, if target manifests have been found to
        validate against but the manifest itself does not contain data (across the entire manifest,
        not just the column being validated.)
        Args:
            dmge: DataModelGraphExplorer object
            attribute_name, str: str, attribute being validated
            val_rule: str, defined in the schema.
        Returns:
            Errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        """
        errors, warnings = [], []
        error_message = (
            f"Cross Manifest Validation: There were manifests found to match for attribute: {attribute_name} "
            f"and validation rule: {val_rule}, but no data was found in this manifest to validate against."
        )

        nv_errors, nv_warnings = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=None,
            error_col=attribute_name,
            error_message=error_message,
            error_val="No Invalid Entry Recorded",
        )
        if nv_errors:
            errors.append(nv_errors)
        if nv_warnings:
            warnings.append(nv_warnings)
        return errors, warnings

    def generate_filename_error(
        val_rule: str,
        attribute_name: str,
        row_num: str,
        invalid_entry: Any,
        error_type: str,
        dmge: DataModelGraphExplorer,
    ) -> tuple[list[str], list[str]]:
        """
        Purpose:
            Generate an logging error as well as a stored error message, when
            a filename error is encountered.
        Args:
            val_rule: str, rule as defined in the schema for the component.
            attribute_name: str, attribute being validated
            row_num: str, row where the error was detected
            invalid_entry: str, value that caused the error
            error_type: str, type of error encountered
            dmge: DataModelGraphExplorer object
        Returns:
            Errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        """
        error_messages = {
            "mismatched entityId": f"The entityId for file path '{invalid_entry}' on row {row_num}"
            " does not match the entityId for the file in the file view.",
            "path does not exist": f"The file path '{invalid_entry}' on row {row_num} does not exist in the file view.",
            "entityId does not exist": f"The entityId for file path '{invalid_entry}' on row {row_num}"
            " does not exist in the file view.",
            "missing entityId": f"The entityId is missing for file path '{invalid_entry}' on row {row_num}.",
        }
        error_message = error_messages.get(error_type, None)
        if not error_message:
            raise KeyError(f"Unsupported error type provided: '{error_type}'")

        error_list, warning_list = GenerateError.raise_and_store_message(
            dmge=dmge,
            val_rule=val_rule,
            error_row=row_num,
            error_col=attribute_name,
            error_message=error_message,
            error_val=invalid_entry,
        )

        return error_list, warning_list

    def _get_rule_attributes(
        val_rule: str, error_col_name: str, dmge: DataModelGraphExplorer
    ) -> tuple[list, str, MessageLevelType, bool, bool, bool]:
        """Extract different attributes from the given rule
        Args:
            val_rule, str: validation_rule being passed.
            error_col_name, str, the display name of the attribute the rule is being applied to
            dmge, DataModelGraphExplorer Object
        Returns:
        """
        rule_parts = val_rule.split(" ")
        rule_name = rule_parts[0]
        specified_level = rule_parts[-1].lower()
        specified_level = (
            specified_level if specified_level in ["warning", "error"] else None
        )

        is_schema_error = rule_name == "schema"
        col_is_recommended = rule_name == "recommended"

        if not is_schema_error:
            col_is_required = dmge.get_node_required(node_display_name=error_col_name)
        else:
            col_is_required = False

        return (
            rule_parts,
            rule_name,
            specified_level,
            is_schema_error,
            col_is_recommended,
            col_is_required,
        )

    def get_is_na_allowed(node_display_name: str, dmge: DataModelGraphExplorer) -> bool:
        """Determine if NAs are allowed based on the original set of rules
        Args:
            node_display_name, str: display name for the current attribure
            dmge, DataModelGraphExplorer
        Returns:
            bool: True, if IsNA is one of the rules, else False
        """
        # Get -all- of the specified validation rules for the attribute,
        validation_rule_list = dmge.get_node_validation_rules(
            node_display_name=node_display_name
        )

        # Determine if IsNA is one of the rules, if it is return True, else return False
        if rule_in_rule_list("IsNA", validation_rule_list):
            return True
        else:
            return False

    def get_error_value_is_na(
        error_val,
        na_allowed: bool = False,
    ) -> bool:
        """Determine if the erroring value is NA
        Args:
            error_val: erroneous value
        Returns:
            bool: Returns True, if the error value is evaluated to be NA, and False if not
        ):

        """
        not_applicable_strings = [
            "not applicable",
        ]

        # Try to figure out if the erroring value is NA
        if isinstance(error_val, str) and na_allowed:
            error_val_is_na = error_val.lower() in not_applicable_strings
        elif isinstance(error_val, list):
            error_val_is_na = False
        elif (error_val is None) or pd.isnull(error_val) or (error_val == "<NA>"):
            error_val_is_na = True
        else:
            error_val_is_na = False
        return error_val_is_na

    def _determine_messaging_level(
        rule_name: str,
        error_val_is_na: bool,
        specified_level: MessageLevelType,
        is_schema_error: bool,
        col_is_required: bool,
        col_is_recommended: bool,
    ) -> Optional[MessageLevelType]:
        """Deterimine messaging level given infromation that was gathered about the rule and the error value
        Args:
            rule_name, str: The name of the rule being applied to the data, stripped of additional information
            error_val_is_na, bool: True if error is entry is non-value, False if entry has a value
            specified_level, MessageLevelType: Messaging level specified in the rule.
            is_schema_error, bool: True if rule_name=="schema"
            col_is_required, bool: True if the attribute column is required in the schema.
            col_is_recommended, bool: True if rule_name=="recommended"
        Returns:
            Optional[MessageLevelType]: Messaging level is returned, if applicable.
        """

        # If the erroring value is NA, do not raise message. Do not worry about if value is required or not
        # bc this is already accounted for in JSON Validation.
        # This allows flexibiity to where, we dont need the value to be provided,
        # but also the case where NA is recorded as the value 'Not Applicable'
        if error_val_is_na and col_is_recommended:
            message_level = "warning"

        elif error_val_is_na:
            message_level = None

        # If the level was specified, return that level
        elif specified_level:
            message_level = specified_level

        # If the rule being evaluated IsNa then do not raise message
        elif rule_name.lower() == "isna":
            message_level = None

        # If schema error return Error
        elif is_schema_error:
            message_level = "error"

        # If the column is not required, raise a warning,
        elif not col_is_required:
            message_level = "warning"
        # If is the column is required, but recommended, do not raise message
        elif col_is_required and col_is_recommended:
            message_level = None

        # If none of the above statements catches, then return the default message level, determine for a given rule.
        # Rules have default messaging levels.

        else:
            message_level = validation_rule_info()[rule_name]["default_message_level"]
        return message_level

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
                4. If no level is specified and there is an erroneous value, level is determined by whether or not
                the attribute is required and if the rule set is modified by the recommended modifier.
                5. If none of the above conditions apply, the default message level for the rule is logged.
        Input:
                dmge: DataModelGraphExplorer object
                error_col: str, Display name of attribute being validated
                error_val: Any, erroneous value
                val_rule: str, current attribute rule, defined in the schema, and being evaluated in this step
                    (single rule set)
        Returns:
            'error', 'warning' or None
        Raises:
            Logging messagees, either error, warning, or no message
        """

        # Extract attributes from the current val_rule
        (
            rule_parts,
            rule_name,
            specified_level,
            is_schema_error,
            col_is_recommended,
            col_is_required,
        ) = GenerateError._get_rule_attributes(
            val_rule=val_rule, error_col_name=error_col, dmge=dmge
        )

        # Determine if NA values are allowed.
        na_allowed = GenerateError.get_is_na_allowed(
            node_display_name=error_col, dmge=dmge
        )

        # Determine if the provided value that is
        error_val_is_na = GenerateError.get_error_value_is_na(
            error_val,
            na_allowed,
        )
        # Return Messaging Level as appropriate, determined based on logic and heirarchy
        message_level = GenerateError._determine_messaging_level(
            rule_name,
            error_val_is_na,
            specified_level,
            is_schema_error,
            col_is_required,
            col_is_recommended,
        )

        return message_level

    def raise_and_store_message(
        dmge: DataModelGraphExplorer,
        val_rule: str,
        error_row: Optional[str],
        error_col: Optional[str],
        error_message: str,
        error_val: Union[str, list[str]],
        message_level: Optional[str] = None,
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
            - message_level: str, message level to raise, if its an unchanging level.
        Raises:
            logger.error or logger.warning or no message
        Returns:
            error_list: list of errors
            warning_list: list of warnings
        """

        error_list = []
        warning_list = []

        if not message_level:
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

        if error_val == "No Invalid Entry Recorded":
            error_val = None

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

    def _login(
        self,
        access_token: Optional[str] = None,
        project_scope: Optional[list[str]] = None,
        columns: Optional[list] = None,
        where_clauses: Optional[list] = None,
    ):
        # if the ValidateAttribute object already has a SynapseStorage object, just requery the fileview, if not then login
        if hasattr(self, "synStore"):
            if self.synStore.project_scope != project_scope:
                self.synStore.project_scope = project_scope
            self.synStore.query_fileview(columns=columns, where_clauses=where_clauses)
        else:
            try:
                self.synStore = SynapseStorage(
                    access_token=access_token,
                    project_scope=project_scope,
                    columns=columns,
                    where_clauses=where_clauses,
                )
            except SynapseNoCredentialsError as e:
                raise ValueError(
                    "No Synapse credentials were provided. Credentials must be provided to utilize cross-manfiest validation functionality."
                ) from e

    def get_no_entry(self, entry: str, node_display_name: str) -> bool:
        """Helper function to check if the entry is blank or contains a not applicable type string (and NA is permitted)
        Args:
            entry, str: manifest entry currently under evaluation
            node_display_name, str: node display name of the attribute currently under evaluation
        Returns:
            True, if value_is_na and na allowed
            False, if entry has a value, or if submitted not applicable string is not allowed.
        """
        na_allowed = GenerateError.get_is_na_allowed(
            node_display_name=node_display_name, dmge=self.dmge
        )
        value_is_na = GenerateError.get_error_value_is_na(
            entry,
            na_allowed,
        )
        if value_is_na:
            return True
        else:
            return False

    def get_entry_has_value(
        self,
        entry: str,
        node_display_name: str,
    ) -> bool:
        """Return the inverse of get_no_entry.
        Args:
            entry, str: manifest entry currently under evaluation
            node_display_name, str: node display name of the attribute currently under evaluation
        Returns:
            True, if entry has a value, or if submitted not applicable string is not allowed.
            False, if value_is_na and na allowed
        """
        return not self.get_no_entry(
            entry,
            node_display_name,
        )

    def _get_target_manifest_dataframes(
        self,
        target_component: str,
        project_scope: Optional[list[str]] = None,
        access_token: Optional[str] = None,
    ) -> dict[str, pd.DataFrame]:
        """Returns target manifest dataframes in the form of a dictionary

        Args:
            target_component (str): The component to get manifests for
            project_scope (Optional[list[str]], optional):
             Projects to limit the scope of cross manifest validation to. Defaults to None.
            access_token (Optional[str], optional): Asset Store access token. Defaults to None.

        Returns:
            dict[str, pd.DataFrame]: Keys are synapse ids, values are datframes of the synapse id
        """
        manifest_ids, dataset_ids = self.get_target_manifests(
            target_component, project_scope, access_token
        )
        manifests: list[pd.DataFrame] = []
        for dataset_id in dataset_ids:
            entity: File = self.synStore.getDatasetManifest(
                datasetId=dataset_id, downloadFile=True
            )
            manifests.append(pd.read_csv(entity.path))
        return dict(zip(manifest_ids, manifests))

    def get_target_manifests(
        self,
        target_component: str,
        project_scope: Optional[list[str]],
        access_token: Optional[str] = None,
    ) -> tuple[list[str], list[str]]:
        """Gets a list of synapse ids of mainfests to check against

        Args:
            target_component (str): Manifet ids are gotten fo this type
            project_scope (Optional[list[str]]): Projects to limit the scope
              of cross manifest validation to. Defaults to None.
            access_token (Optional[str], optional): Synapse access token Defaults to None.

        Returns:
            tuple[list[str], list[str]]:
              A list of manifest synapse ids, and their dataset synapse ids
        """
        t_manifest_search = perf_counter()
        target_manifest_ids = []
        target_dataset_ids = []

        self._login(project_scope=project_scope, access_token=access_token)

        # Get list of all projects user has access to
        projects = self.synStore.getStorageProjects(project_scope=project_scope)

        for project in projects:
            # get all manifests associated with datasets in the projects
            target_datasets = self.synStore.getProjectManifests(projectId=project[0])

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
        return target_manifest_ids, target_dataset_ids

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
        replace_null = True

        csv_re = comma_separated_list_regex()

        # Check if lists -must- be a list, or can be a single value.
        list_robustness = get_list_robustness(val_rule=val_rule)

        if list_robustness == "like":
            replace_null = False

        elif list_robustness == "strict":
            manifest_col = manifest_col.astype(str)

            # This will capture any if an entry is not formatted properly. Only for strict lists
            for i, list_string in enumerate(manifest_col):
                list_error = None
                entry_has_value = self.get_entry_has_value(
                    entry=list_string,
                    node_display_name=manifest_col.name,
                )

                if not isinstance(list_string, str) and entry_has_value:
                    list_error = "not_a_string"
                elif not re.fullmatch(csv_re, list_string) and entry_has_value:
                    list_error = "not_comma_delimited"

                if list_error:
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
        manifest_col = parse_str_series_to_list(manifest_col, replace_null=replace_null)

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
                    entry_has_value = self.get_entry_has_value(
                        entry=re_to_check,
                        node_display_name=manifest_col.name,
                    )
                    if entry_has_value:
                        re_to_check = str(re_to_check)
                        if not bool(
                            module_to_call(reg_expression, re_to_check)
                        ) and bool(re_to_check):
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
                # check if <NA> in list let pass.
                entry_has_value = self.get_entry_has_value(
                    entry=re_to_check,
                    node_display_name=manifest_col.name,
                )

                if (
                    not bool(module_to_call(reg_expression, re_to_check))
                    and bool(re_to_check)
                    and entry_has_value
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
                entry_has_value = self.get_entry_has_value(
                    entry=value,
                    node_display_name=manifest_col.name,
                )
                if (
                    bool(value)
                    and not isinstance(value, specified_type[val_rule])
                    and entry_has_value
                ):
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
                entry_has_value = self.get_entry_has_value(
                    entry=value,
                    node_display_name=manifest_col.name,
                )
                if (
                    bool(value)
                    and not isinstance(value, specified_type[val_rule])
                    and entry_has_value
                ):
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
        manifest_col: pd.Series,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """
        Purpose:
            Validate URL's submitted for a particular attribute in a manifest.
            Determine if the URL is valid and contains attributes specified in the
            schema. Additionally, the server must be reachable to be deemed as valid.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.Series, column for a given
                attribute in the manifest
        Output:
            This function will return errors when the user input value
            does not match schema specifications.
        """

        url_args = val_rule.split(" ")[1:]
        errors = []
        warnings = []

        for i, url in enumerate(manifest_col):
            entry_has_value = self.get_entry_has_value(
                entry=url,
                node_display_name=manifest_col.name,
            )
            if entry_has_value:
                # Check if a random phrase, string or number was added and
                # log the appropriate error. Specifically, Raise an error if the value
                # added is not a string or no part of the string can be parsed as a
                # part of a URL.
                if not isinstance(url, str) or not (
                    urlparse(url).scheme
                    + urlparse(url).netloc
                    + urlparse(url).params
                    + urlparse(url).query
                    + urlparse(url).fragment
                ):
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
                        valid_url = True
                        response = requests.options(url, allow_redirects=True)
                        logger.debug(
                            "Validated URL [URL: %s, status_code: %s]",
                            url,
                            response.status_code,
                        )
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
                    if valid_url:
                        # If the URL works, check to see if it contains the proper arguments
                        # as specified in the schema.
                        for arg in url_args:
                            if arg not in url:
                                url_error = "arg_error"
                                (
                                    vr_errors,
                                    vr_warnings,
                                ) = GenerateError.generate_url_error(
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
            invalid_enties, list: invalid values recorded in the validation log
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
            dict[str, pd.Series],
            list[str],
            dict[str, pd.Series],
        ],
    ) -> tuple[list[str], list[str]]:
        """Based on the cross manifest validation rule, and in set rule scope, pass variables to
        _get_cross_errors_warnings
            to log appropriate error or warning.
        Args:
            val_rule, str: Validation Rule
            source_attribute, str: Source manifest column name
            set_validation_store, tuple[dict[str, pd.Series], list[string],
            dict[str, pd.Series]]:
                contains the missing_manifest_log, present_manifest_log, and repeat_manifest_log
            dmge: DataModelGraphExplorer Object.

        Returns:
            errors, list[str]: list of errors to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
            warnings, list[str]: list of warnings to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
        """
        errors: list[str] = []
        warnings: list[str] = []

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

    def _remove_non_entry_from_invalid_entry_list(
        self,
        invalid_entry: Optional[list[str]],
        row_num: Optional[list[str]],
        attribute_name: str,
    ) -> tuple[list[str], list[str]]:
        """Helper to remove NAs from a list of invalid entries (if applicable, and allowed), remove the row
        too from row_num. This will make sure errors are not rasied for NA entries unless the value is required.
        Args:
            invalid_entry, list[str]: default=None, list of entries in the source manifest where
                invalid values were located.
            row_num, list[str[: default=None, list of rows in the source manifest where invalid values were located
            attribute_name, str: source attribute name
        Returns:
            invalid_entry and row_num returned with any NA and corresponding row index value removed, if applicable.
        """
        idx_to_remove = []
        # Check if the current attribute column is required, via the data model
        if invalid_entry and row_num:
            # Check each invalid entry and determine if it has a value and/or is required.
            # If there is no entry and its not required, remove the NA value so an error is not raised.
            for idx, entry in enumerate(invalid_entry):
                entry_has_value = self.get_entry_has_value(entry, attribute_name)
                # If there is no value, and is not required, recored the index
                if not entry_has_value:
                    idx_to_remove.append(idx)

            # If indices are recorded for NA values to remove, remove them and the corresponding row
            if idx_to_remove:
                for idx in sorted(idx_to_remove, reverse=True):
                    del invalid_entry[idx]
                    del row_num[idx]
                # Perform check to make sure length of invalid_entry and row_num is the same. If not that would suggest
                # there was an issue recording or removing values.
                if len(invalid_entry) != len(row_num):
                    logger.error(
                        f"There was an error handling and validating a non-entry."
                        f"Please try again or contact Schematic administrators."
                    )
        return invalid_entry, row_num

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
            matching_manifests, list: default=None, ist of manifests with all values in the target attribute present
            manifest_id, list: default=None, list of manifests where invalid values were located.
            invalid_entry, list: default=None, list of entries in the source manifest where invalid values were located.
        Returns:
            errors, list[str]: list of errors to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
            warnings, list[str]: list of warnings to raise, as appropriate, if values in current manifest do
            not pass relevant cross mannifest validation across the target manifest(s)
        """
        invalid_entry, row_num = self._remove_non_entry_from_invalid_entry_list(
            invalid_entry, row_num, attribute_name
        )
        errors, warnings = [], []

        # Want to make sure we only generate errors when appropriate. Dont call, if we have removed all Nans,
        # Also rules either require an invalid entry OR matching manifests. So let pass if either of those
        # thresholds are met.
        if invalid_entry or matching_manifests:
            if not invalid_entry:
                invalid_entry = "No Invalid Entry Recorded"
            vr_errors, vr_warnings = GenerateError.generate_cross_warning(
                val_rule=val_rule,
                attribute_name=attribute_name,
                dmge=self.dmge,
                matching_manifests=matching_manifests,
                manifest_id=manifest_id,
                invalid_entry=invalid_entry,
                row_num=row_num,
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
        value_validation_store: tuple[pd.Series, pd.Series, pd.Series],
    ) -> tuple[list[str], list[str]]:
        """For value rule scope, find invalid rows and entries, and generate appropriate errors and warnings
        Args:
            val_rule, str: Validation rule
            source_attribute, str: source manifest column name
            value_validation_store, tuple(pd.Series, pd.Series, pd.Series]):
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

    def _check_if_target_manifest_is_empty(
        self,
        target_manifest: pd.core.series.Series,
        target_manifest_empty: list[bool],
        column_names: dict[str, str],
    ) -> list[bool]:
        """If a target manifest is found with the attribute column of interest check to see if the manifest is empty.
        Args:
            target_manifest, pd.core.series.Series: Current target manifest
            target_manifest_empty, list[bool]: a list of booleans recording if the target manifest are emtpy or not.
            column_names, dict[str, str]: {stripped_col_name:original_column_name}
        Returns:
            target_manifest_empty, list[bool]: a list of booleans recording if the target manifest are emtpy or not.
        """
        # Make a copy of the target manifest with only user uploaded columns
        target_manifest_dupe = target_manifest.drop(
            [column_names["component"], column_names["id"], column_names["entityid"]],
            axis=1,
        )

        # Check if the entire target_manifest is empty
        if sum(target_manifest_dupe.isnull().values.all(axis=0)) == len(
            target_manifest_dupe.columns
        ):
            target_manifest_empty.append(True)
        else:
            target_manifest_empty.append(False)
        return target_manifest_empty

    def _run_validation_across_targets_set(
        self,
        val_rule: str,
        column_names: dict[str, str],
        manifest_col: pd.Series,
        target_attribute: str,
        target_manifest: pd.DataFrame,
        target_manifest_id: str,
        missing_manifest_log: dict[str, pd.Series],
        present_manifest_log: list[str],
        repeat_manifest_log: dict[str, pd.Series],
        target_attribute_in_manifest_list: list[bool],
        target_manifest_empty: list[bool],
    ) -> tuple[
        tuple[
            dict[str, pd.Series],
            list[str],
            dict[str, pd.Series],
        ],
        list[bool],
        list[bool],
    ]:
        """For set rule scope, go through the given target column and look
        Args:
            val_rule, str: Validation rule
            column_names, dict[str,str]: {stripped_col_name:original_column_name}
            target_column, pd.Series: Empty target_column to fill out in this function
            manifest_col, pd.Series: Source manifest column
            target_attribute, str: current target attribute
            target_column, pd.Series: Current target column
            target_manifest, pd.DataFrame: Current target manifest
            target_manifest_id, str: Current target manifest Synapse ID
            missing_manifest_log, dict[str, pd.Series]:
                Log of manifests with missing values, {synapse_id: index,missing value}, updated.
            present_manifest_log, list[str]
                Log of present manifests, [synapse_id present manifest], updated.
            repeat_manifest_log, dict[str, pd.Series]
                Log of manifests with repeat values, {synapse_id: index,repeat value}, updated.

        Returns:
            tuple(
            missing_manifest_log, dict[str, pd.Series]:
                Log of manifests with missing values, {synapse_id: index,missing value}, updated.
            present_manifest_log, list[str]
                Log of present manifests, [synapse_id present manifest], updated.
            repeat_manifest_log, dict[str, pd.Series]
                Log of manifests with repeat values, {synapse_id: index,repeat value}, updated.)
            target_attribute_in_manifest, bool: True if the target attribute is in the current manifest.
        """
        target_attribute_in_manifest = False
        # If the manifest has the target attribute for the component do the cross validation
        if target_attribute in column_names:
            target_attribute_in_manifest = True
            target_column = target_manifest[column_names[target_attribute]]

            target_manifest_empty = self._check_if_target_manifest_is_empty(
                target_manifest=target_manifest,
                target_manifest_empty=target_manifest_empty,
                column_names=column_names,
            )

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
        target_attribute_in_manifest_list.append(target_attribute_in_manifest)
        return (
            (
                missing_manifest_log,
                present_manifest_log,
                repeat_manifest_log,
            ),
            target_attribute_in_manifest_list,
            target_manifest_empty,
        )

    def _gather_target_columns_value(
        self,
        column_names: dict[str, str],
        target_attribute: str,
        concatenated_target_column: pd.Series,
        target_manifest: pd.DataFrame,
        target_attribute_in_manifest_list: list[bool],
        target_manifest_empty: list[bool],
    ) -> tuple[pd.Series, list[bool], list[bool],]:
        """A helper function for creating a concatenating all target attribute columns across all target manifest.
            This function checks if the target attribute is in the current target manifest. If it is, and is the
            first manifest with this column, start recording it, if it has already been recorded from
            another manifest concatenate the new column to the concatenated_target_column series.
        Args:
            column_names, dict: {stripped_col_name:original_column_name}
            target_attribute, str: current target attribute
            concatenated_target_column, pd.Series: target column in the process of being built, possibly
                passed through this function multiple times based on the number of manifests
            target_manifest, pd.DataFrame: current target manifest
        Returns:
            concatenated_target_column, pd.Series: All target columns concatenated into a single column
        """
        # Check if the target_attribute is in the current target manifest.
        target_attribute_in_manifest = False
        if target_attribute in column_names:
            target_attribute_in_manifest = True

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

        target_manifest_empty = self._check_if_target_manifest_is_empty(
            target_manifest, target_manifest_empty, column_names
        )
        target_attribute_in_manifest_list.append(target_attribute_in_manifest)
        return (
            concatenated_target_column,
            target_attribute_in_manifest_list,
            target_manifest_empty,
        )

    def _run_validation_across_targets_value(
        self,
        manifest_col: pd.Series,
        concatenated_target_column: pd.Series,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """Get missing values, duplicated values and repeat values assesed comapring the source manifest to all
            the values in all target columns.
        Args:
            manifest_col, pd.Series: Current source manifest column
            concatenated_target_column, pd.Series: All target columns concatenated into a single column
        Returns:
            missing_values, pd.Series: values that are present in the source manifest, but not present
                in the target manifest
            duplicated_values, pd.Series: values that duplicated in the concatenated target column, and
                also present in the source manifest column
            repeat_values, pd.Series: values that are repeated between the manifest column and
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

    def _get_column_names(self, target_manifest: pd.DataFrame) -> dict[str, str]:
        """Convert manifest column names into validation rule input format
        Args:
            target_manifest, pd.DataFrame: Current target manifest
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
        rule_scope: ScopeTypes,
        val_rule: str,
        manifest_col: pd.Series,
        target_column: pd.Series,
        access_token: Optional[str] = None,
        project_scope: Optional[list[str]] = None,
    ) -> tuple[
        float,
        Union[
            tuple[
                dict[str, pd.Series],
                list[str],
                dict[str, pd.Series],
            ],
            tuple[pd.Series, pd.Series, pd.Series],
            bool,
            str,
        ],
    ]:
        """Run cross manifest validation from a source manifest, across all relevant target manifests,
            based on scope. Output start time and validation outputs..
        Args:
            project_scope, Optional[list]: Projects to limit the scope of cross manifest validation to.
            rule_scope, ScopeTypes: The scope of the rule, taken from validation rule
            access_token, Optional[str]: Asset Store access token
            val_rule, str: Validation rule.
            manifest_col, pd.Series: Source manifest column for a given source component
            target_column, pd.Series: Empty target_column to fill out in this function
        Returns:
            start_time, float: start time in fractional seconds
            valdiation_output:
                Union:
                    target_attribute_in_manifest, bool: will return a false boolean if no target manfiest are found.
                    "values not recorded in targets stored", str, will return a string if targets were found, but there
                        was no data in the target.
                    Union[
                        tuple[dict[str, pd.Series], list[str], dict[str, pd.Series]],
                        tuple[dict[str, pd.Series], dict[str, pd.Series],
                            dict[str, pd.Series]]:
                            validation outputs, exact types depend on scope,
        """
        # Initialize variables
        present_manifest_log = []
        duplicated_values = {}
        missing_values = {}
        repeat_values = {}
        missing_manifest_log = {}
        repeat_manifest_log = {}
        target_attribute_in_manifest_list = []
        target_manifest_empty = []

        target_attribute_in_manifest = False

        # Set relevant parameters
        [target_component, target_attribute] = val_rule.lower().split(" ")[1].split(".")
        target_column.name = target_attribute

        # Start timer
        start_time = perf_counter()

        manifest_dict = self._get_target_manifest_dataframes(
            target_component, project_scope, access_token
        )

        # For each target manifest, gather target manifest column and compare to the source manifest column
        # Save relevant data as appropriate for the given scope
        for target_manifest_id, target_manifest in manifest_dict.items():
            # Get manifest column names
            column_names = self._get_column_names(target_manifest=target_manifest)

            # Read each target manifest and run validation of current manifest column (set) against each
            # manifest individually, gather results
            if "set" in rule_scope:
                (
                    (
                        missing_manifest_log,
                        present_manifest_log,
                        repeat_manifest_log,
                    ),
                    target_attribute_in_manifest_list,
                    target_manifest_empty,
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
                    target_attribute_in_manifest_list=target_attribute_in_manifest_list,
                    target_manifest_empty=target_manifest_empty,
                )

            # Concatenate target manifest columns, in a subsequent step will run cross manifest validation from
            # the current manifest
            # column values against the concatenated target column
            if "value" in rule_scope:
                (
                    target_column,
                    target_attribute_in_manifest_list,
                    target_manifest_empty,
                ) = self._gather_target_columns_value(
                    column_names=column_names,
                    target_attribute=target_attribute,
                    concatenated_target_column=target_column,
                    target_manifest=target_manifest,
                    target_attribute_in_manifest_list=target_attribute_in_manifest_list,
                    target_manifest_empty=target_manifest_empty,
                )

        if len(target_attribute_in_manifest_list) > 0:
            if sum(target_attribute_in_manifest_list) == 0:
                target_attribute_in_manifest = False
            else:
                target_attribute_in_manifest = True

        # Check if a target manifest has been found, if so do not export validation_store
        if not target_attribute_in_manifest:
            return (start_time, target_attribute_in_manifest)
        elif sum(target_manifest_empty) > 0:
            return (start_time, "values not recorded in targets stored")
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
        manifest_col: pd.Series,
        project_scope: Optional[list[str]] = None,
        access_token: Optional[str] = None,
    ) -> list[list[str]]:
        """
        Purpose:
            Do cross validation between the current manifest and all other manifests in a given asset view (limited
                by project scope, if provided).
        Args:
            val_rule, str: Validation rule
            manifest_col, pd.Series: column for a given
                attribute in the manifest
            project_scope, Optional[list] = None: Projects to limit the scope of cross manifest validation to.
            dmge: DataModelGraphExplorer Object
            access_token, Optional[str]: Asset Store access token
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
            warnings = GenerateError.generate_no_cross_warning(
                dmge=self.dmge, attribute_name=manifest_col.name, val_rule=val_rule
            )
        elif (
            isinstance(validation_output, str)
            and validation_output == "values not recorded in targets stored"
        ):
            errors, warnings = GenerateError.generate_no_value_in_manifest_error(
                dmge=self.dmge, attribute_name=manifest_col.name, val_rule=val_rule
            )

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

    def filename_validation(
        self,
        val_rule: str,
        manifest: pd.core.frame.DataFrame,
        access_token: str,
        dataset_scope: str,
        project_scope: Optional[list] = None,
    ):
        """
        Purpose:
            Validate the filenames in the manifest against the data paths in the fileview.
        Args:
            val_rule: str, Validation rule for the component
            manifest: pd.core.frame.DataFrame, manifest
            access_token: str, Asset Store access token
            dataset_scope: str, Dataset with files to validate against
            project_scope: Optional[list] = None: Projects to limit the scope of cross manifest validation to.
        Returns:
            errors: list[str] Error details for further storage.
            warnings: list[str] Warning details for further storage.
        """

        if dataset_scope is None:
            raise ValueError(
                "A dataset is required to be specified for filename validation"
            )

        errors = []
        warnings = []

        where_clauses = []

        dataset_clause = f"parentId='{dataset_scope}'"
        where_clauses.append(dataset_clause)

        self._login(
            project_scope=project_scope,
            access_token=access_token,
            columns=["id", "path"],
            where_clauses=where_clauses,
        )

        fileview = self.synStore.storageFileviewTable.reset_index(drop=True)
        # filename in dataset?
        files_in_view = manifest["Filename"].isin(fileview["path"])
        entity_ids_in_view = manifest["entityId"].isin(fileview["id"])
        # filenames match with entity IDs in dataset
        joined_df = manifest.merge(
            fileview, how="left", left_on="Filename", right_on="path"
        )

        entity_id_match = joined_df["id"] == joined_df["entityId"]

        # update manifest with types of errors identified
        manifest_with_errors = deepcopy(manifest)
        manifest_with_errors["Error"] = pd.NA
        manifest_with_errors.loc[~entity_id_match, "Error"] = "mismatched entityId"
        manifest_with_errors.loc[~files_in_view, "Error"] = "path does not exist"
        manifest_with_errors.loc[
            ~entity_ids_in_view, "Error"
        ] = "entityId does not exist"
        manifest_with_errors.loc[
            (manifest_with_errors["entityId"].isna())
            | (manifest_with_errors["entityId"] == ""),
            "Error",
        ] = "missing entityId"

        # Generate errors
        invalid_entries = manifest_with_errors.loc[
            manifest_with_errors["Error"].notna()
        ]
        for index, data in invalid_entries.iterrows():
            vr_errors, vr_warnings = GenerateError.generate_filename_error(
                val_rule=val_rule,
                attribute_name="Filename",
                # +2 to make consistent with other validation functions
                row_num=str(index + 2),
                invalid_entry=data["Filename"],
                error_type=data["Error"],
                dmge=self.dmge,
            )
            if vr_errors:
                errors.append(vr_errors)
            if vr_warnings:
                warnings.append(vr_warnings)
        return errors, warnings
