import logging
import re
from time import perf_counter

# allows specifying explicit variable types
from typing import Optional, Union, Literal, Any
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

MessageLevelType = Literal["warning", "error"]


class GenerateError:
    def generate_schema_error(
        row_num: str,
        attribute_name: str,
        error_message: str,
        invalid_entry,
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
        list_error: Literal["not_comma_delimited", "not_a_string"],
        invalid_entry,
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
        invalid_entry,
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
        invalid_entry,
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
        invalid_entry,
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
        matching_manifests=[],
        missing_manifest_ID=None,
        invalid_entry="No Invalid Entry Recorded",
        row_num=None,
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
            manifest_ID: str, synID of the target manifest missing the source value
            invalid_entry: str, value present in source manifest that is missing in the target
            row_num: row in source manifest with value missing in target manifests
        Returns:
        Errors: list[str] Error details for further storage.
        warnings: list[str] Warning details for further storage.
        """

        if val_rule.__contains__("matchAtLeast"):
            error_message = f"Value(s) {invalid_entry} from row(s) {row_num} of the attribute {attribute_name} in the source manifest are missing."
            error_message += (
                f" Manifest(s) {missing_manifest_ID} are missing the value(s)."
                if missing_manifest_ID
                else ""
            )

        elif val_rule.__contains__("matchExactly"):
            if matching_manifests != []:
                error_message = f"All values from attribute {attribute_name} in the source manifest are present in {len(matching_manifests)} manifests instead of only 1."
                error_message += (
                    f" Manifests {matching_manifests} match the values in the source attribute."
                    if matching_manifests
                    else ""
                )

            elif val_rule.__contains__("set"):
                error_message = f"No matches for the values from attribute {attribute_name} in the source manifest are present in any other manifests instead of being present in exactly 1. "
            elif val_rule.__contains__("value"):
                error_message = f"Value(s) {invalid_entry} from row(s) {row_num} of the attribute {attribute_name} in the source manifest are not present in only one other manifest. "

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
            invalid_entry = iterable_to_str_list(set(invalid_entry)) if invalid_entry else None
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

    def _get_rule_attributes(
        val_rule: str, error_col_name: str, dmge: DataModelGraphExplorer
    ) -> tuple[[str, str, str]]:
        """Extract different attributes from the given rule
        Args:
            val_rule, str:
            error_col_name, str
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
        col_is_required = dmge.get_node_required(node_display_name=error_col_name)
        return (
            rule_parts,
            rule_name,
            specified_level,
            is_schema_error,
            col_is_recommended,
            col_is_required,
        )

    def _get_is_na_allowed(
        node_display_name: str, dmge: DataModelGraphExplorer
    ) -> bool:
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

    def _get_error_value_is_na(
        error_val,
        na_allowed: bool = False,
        col_is_required: bool = False,
    ) -> bool:
        """Determine if the erroring value is NA
        Args:
            error_val: erroneous value
        Returns:
            bool: Returns True, if the error value is evaluated to be NA, and False if not
        """
        not_applicable_strings = [
            "not applicable",
        ]

        # Try to figure out if the erroring value is NA
        if isinstance(error_val, str) and na_allowed:
            error_val_is_na = error_val.lower() in not_applicable_strings
        elif isinstance(error_val, list):
            error_val_is_na = False
        elif (
            (error_val is None)
            or pd.isnull(error_val)
            or (error_val == "<NA>" and not col_is_required)
        ):
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
            rule_name, str:
            error_val_is_na, bool:
            specified_level, MessageLevelType:
            is_schema_error, bool:
            col_is_required, bool:
            col_is_recommended, bool:
        Returns:
            Optional[MessageLevelType]:
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
        error_val,
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
        na_allowed = GenerateError._get_is_na_allowed(
            node_display_name=error_col, dmge=dmge
        )

        # Determine if the provided value that is
        error_val_is_na = GenerateError._get_error_value_is_na(
            error_val, na_allowed, col_is_required
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
        error_row: str,
        error_col: str,
        error_message: str,
        error_val,
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

        if error_val == "No Invalid Entry Recorded":
            error_val=None

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

    def get_no_entry_and_not_required(dmge, entry, node_display_name, col_is_required):
        # check if <NA> in list let pass if not required and no value is recorded.
        no_entry_and_not_required = False
        if not col_is_required:
            na_allowed = GenerateError._get_is_na_allowed(
                node_display_name=node_display_name, dmge=dmge
            )
            value_is_na = GenerateError._get_error_value_is_na(
                entry, na_allowed, col_is_required
            )
            if value_is_na:
                no_entry_and_not_required = True

        return no_entry_and_not_required

    def get_entry_has_value_or_required(
        dmge, entry, node_display_name, col_is_required
    ):
        return not ValidateAttribute.get_no_entry_and_not_required(
            dmge, entry, node_display_name, col_is_required
        )

    def get_target_manifests(
        target_component, project_scope: list, access_token: str = None
    ):
        t_manifest_search = perf_counter()
        target_manifest_IDs = []
        target_dataset_IDs = []

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
                    target_manifest_IDs.append(target_dataset[1][0])
                    target_dataset_IDs.append(target_dataset[0][0])

        logger.debug(
            f"Cross manifest gathering elapsed time {perf_counter()-t_manifest_search}"
        )
        return synStore, target_manifest_IDs, target_dataset_IDs

    def list_validation(
        self,
        val_rule: str,
        manifest_col: pd.core.series.Series,
        dmge: DataModelGraphExplorer,
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

        csv_re = comma_separated_list_regex()

        rule_parts = val_rule.lower().split(" ")
        if len(rule_parts) > 1:
            list_robustness = rule_parts[1]
        else:
            list_robustness = "strict"

        if list_robustness == "strict":
            manifest_col = manifest_col.astype(str)

            col_is_required = dmge.get_node_required(
                node_display_name=manifest_col.name
            )
            # This will capture any if an entry is not formatted properly. Only for strict lists
            for i, list_string in enumerate(manifest_col):
                list_error = None
                entry_has_value_or_is_required = (
                    ValidateAttribute.get_entry_has_value_or_required(
                        dmge=dmge,
                        entry=list_string,
                        node_display_name=manifest_col.name,
                        col_is_required=col_is_required,
                    )
                )

                if not isinstance(list_string, str) and entry_has_value_or_is_required:
                    list_error = "not_a_string"
                elif (
                    not re.fullmatch(csv_re, list_string)
                    and entry_has_value_or_is_required
                ):
                    list_error = "not_comma_delimited"
                    vr_errors, vr_warnings = GenerateError.generate_list_error(
                        list_string,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        list_error=list_error,
                        invalid_entry=manifest_col[i],
                        dmge=dmge,
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
        dmge: DataModelGraphExplorer,
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

        validation_rules = dmge.get_node_validation_rules(
            node_display_name=manifest_col.name
        )
        col_is_required = dmge.get_node_required(node_display_name=manifest_col.name)
        if validation_rules and "::" in validation_rules[0]:
            validation_rules = validation_rules[0].split("::")
        # Handle case where validating re's within a list.
        if re.search("list", "|".join(validation_rules)):
            if type(manifest_col[0]) == str:
                # Convert string to list.
                manifest_col = parse_str_series_to_list(manifest_col)

            for i, row_values in enumerate(manifest_col):
                for j, re_to_check in enumerate(row_values):
                    entry_has_value_or_is_required = (
                        ValidateAttribute.get_entry_has_value_or_required(
                            dmge=dmge,
                            entry=re_to_check,
                            node_display_name=manifest_col.name,
                            col_is_required=col_is_required,
                        )
                    )
                    if entry_has_value_or_is_required:
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
                                dmge=dmge,
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
                entry_has_value_or_is_required = (
                    ValidateAttribute.get_entry_has_value_or_required(
                        dmge=dmge,
                        entry=re_to_check,
                        node_display_name=manifest_col.name,
                        col_is_required=col_is_required,
                    )
                )

                if (
                    not bool(module_to_call(reg_expression, re_to_check))
                    and bool(re_to_check)
                    and entry_has_value_or_is_required
                ):
                    vr_errors, vr_warnings = GenerateError.generate_regex_error(
                        val_rule=val_rule,
                        reg_expression=reg_expression,
                        row_num=str(i + 2),
                        module_to_call=reg_exp_rules[1],
                        attribute_name=manifest_col.name,
                        invalid_entry=manifest_col[i],
                        dmge=dmge,
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
        dmge: DataModelGraphExplorer,
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
            - dmge: DataModelGraphExplorer Object
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
        col_is_required = dmge.get_node_required(node_display_name=manifest_col.name)

        # num indicates either a float or int.
        if val_rule == "num":
            for i, value in enumerate(manifest_col):
                entry_has_value_or_is_required = (
                    ValidateAttribute.get_entry_has_value_or_required(
                        dmge=dmge,
                        entry=value,
                        node_display_name=manifest_col.name,
                        col_is_required=col_is_required,
                    )
                )
                if (
                    bool(value)
                    and not isinstance(value, specified_type[val_rule])
                    and entry_has_value_or_is_required
                ):
                    vr_errors, vr_warnings = GenerateError.generate_type_error(
                        val_rule=val_rule,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        invalid_entry=str(manifest_col[i]),
                        dmge=dmge,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)
        elif val_rule in ["int", "float", "str"]:
            for i, value in enumerate(manifest_col):
                entry_has_value_or_is_required = (
                    ValidateAttribute.get_entry_has_value_or_required(
                        dmge=dmge,
                        entry=value,
                        node_display_name=manifest_col.name,
                        col_is_required=col_is_required,
                    )
                )
                if (
                    bool(value)
                    and not isinstance(value, specified_type[val_rule])
                    and entry_has_value_or_is_required
                ):
                    vr_errors, vr_warnings = GenerateError.generate_type_error(
                        val_rule=val_rule,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        invalid_entry=str(manifest_col[i]),
                        dmge=dmge,
                    )
                    if vr_errors:
                        errors.append(vr_errors)
                    if vr_warnings:
                        warnings.append(vr_warnings)
        return errors, warnings

    def url_validation(
        self, val_rule: str, manifest_col: str, dmge: DataModelGraphExplorer
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
            - dmge: DataModelGraphExplorer Object
        Output:
            This function will return errors when the user input value
            does not match schema specifications.
        """

        url_args = val_rule.split(" ")[1:]
        errors = []
        warnings = []

        col_is_required = dmge.get_node_required(node_display_name=manifest_col.name)

        for i, url in enumerate(manifest_col):
            entry_has_value_or_is_required = (
                ValidateAttribute.get_entry_has_value_or_required(
                    dmge=dmge,
                    entry=url,
                    node_display_name=manifest_col.name,
                    col_is_required=col_is_required,
                )
            )
            if entry_has_value_or_is_required:
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
                        dmge=dmge,
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
                            dmge=dmge,
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
                                    dmge=dmge,
                                    val_rule=val_rule,
                                )
                                if vr_errors:
                                    errors.append(vr_errors)
                                if vr_warnings:
                                    warnings.append(vr_warnings)
        return errors, warnings

    def cross_validation(
        self,
        val_rule: str,
        manifest_col: pd.core.series.Series,
        project_scope: list,
        dmge: DataModelGraphExplorer,
        access_token: str,
    ) -> list[list[str]]:
        """
        Purpose:
            Do cross validation between the current manifest and all other manifests a user has access to on Synapse.
            Check if values in this manifest are present fully in others.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given
                attribute in the manifest
            - dmge: DataModelGraphExplorer Object
        Output:
            This function will return errors when values in the current manifest's attribute
            are not fully present in the correct amount of other manifests.
        """
        errors = []
        warnings = []
        missing_values = {}
        missing_manifest_log = {}
        present_manifest_log = []
        target_column = pd.Series(dtype=object)
        # parse sources and targets
        source_attribute = manifest_col.name
        [target_component, target_attribute] = val_rule.lower().split(" ")[1].split(".")
        scope = val_rule.lower().split(" ")[2]
        target_column.name = target_attribute
        col_is_required = dmge.get_node_required(node_display_name=manifest_col.name)

        # Get IDs of manifests with target component
        (
            synStore,
            target_manifest_IDs,
            target_dataset_IDs,
        ) = ValidateAttribute.get_target_manifests(
            target_component, project_scope, access_token
        )

        t_cross_manifest = perf_counter()
        # Read each manifest
        for target_manifest_ID, target_dataset_ID in zip(
            target_manifest_IDs, target_dataset_IDs
        ):
            entity = synStore.getDatasetManifest(
                datasetId=target_dataset_ID, downloadFile=True
            )
            target_manifest = pd.read_csv(entity.path)

            # convert manifest column names into validation rule input format -
            column_names = {}
            for name in target_manifest.columns:
                column_names[name.replace(" ", "").lower()] = name

            if scope.__contains__("set"):
                # If the manifest has the target attribute for the component do the cross validation
                if target_attribute in column_names:
                    target_column = target_manifest[column_names[target_attribute]]

                    # Do the validation on both columns
                    missing_values = manifest_col[~manifest_col.isin(target_column)]

                    if not col_is_required:
                        missing_values.dropna(inplace=True)

                    if missing_values.empty:
                        present_manifest_log.append(target_manifest_ID)
                    else:
                        missing_manifest_log[target_manifest_ID] = missing_values

            elif scope.__contains__("value"):
                if target_attribute in column_names:
                    target_manifest.rename(
                        columns={column_names[target_attribute]: target_attribute},
                        inplace=True,
                    )

                    target_column = pd.concat(
                        objs=[target_column, target_manifest[target_attribute]],
                        join="outer",
                        ignore_index=True,
                    )
                    target_column = target_column.astype("object")
                    # print(target_column)

        missing_rows = []
        missing_values = []

        if scope.__contains__("value"):
            missing_values = manifest_col[~manifest_col.isin(target_column)]
            duplicated_values = manifest_col[
                manifest_col.isin(target_column[target_column.duplicated()])
            ]
            if not col_is_required:
                missing_values.dropna(inplace=True)
                duplicated_values.dropna(inplace=True)

            if val_rule.__contains__("matchAtLeastOne") and not missing_values.empty:
                missing_rows = missing_values.index.to_numpy() + 2
                missing_rows = np_array_to_str_list(missing_rows)

                vr_errors, vr_warnings = GenerateError.generate_cross_warning(
                    val_rule=val_rule,
                    row_num=missing_rows,
                    attribute_name=source_attribute,
                    invalid_entry=iterable_to_str_list(missing_values),
                    dmge=dmge,
                )
                if vr_errors:
                    errors.append(vr_errors)
                if vr_warnings:
                    warnings.append(vr_warnings)
            elif val_rule.__contains__("matchExactlyOne") and (
                duplicated_values.any() or missing_values.any()
            ):
                invalid_values = pd.merge(
                    duplicated_values, missing_values, how="outer"
                )

                invalid_rows = (
                    pd.merge(
                        duplicated_values,
                        missing_values,
                        how="outer",
                        left_index=True,
                        right_index=True,
                    ).index.to_numpy()
                    + 2
                )
                invalid_rows = np_array_to_str_list(invalid_rows)
                vr_errors, vr_warnings = GenerateError.generate_cross_warning(
                    val_rule=val_rule,
                    row_num=invalid_rows,
                    attribute_name=source_attribute,
                    invalid_entry=iterable_to_str_list(invalid_values.squeeze()),
                    dmge=dmge,
                )
                if vr_errors:
                    errors.append(vr_errors)
                if vr_warnings:
                    warnings.append(vr_warnings)

        # generate warnings if necessary
        elif scope.__contains__("set"):
            if (
                val_rule.__contains__("matchAtLeastOne")
                and len(present_manifest_log) < 1
            ):
                missing_entries = list(missing_manifest_log.values())
                missing_manifest_IDs = list(missing_manifest_log.keys())

                for missing_entry in missing_entries:
                    missing_rows.append(missing_entry.index[0] + 2)
                    missing_values.append(missing_entry.values[0])

                missing_rows = iterable_to_str_list(set(missing_rows))
                missing_values = iterable_to_str_list(set(missing_values))

                vr_errors, vr_warnings = GenerateError.generate_cross_warning(
                    val_rule=val_rule,
                    row_num=missing_rows,
                    attribute_name=source_attribute,
                    invalid_entry=missing_values,
                    missing_manifest_ID=missing_manifest_IDs,
                    dmge=dmge,
                )
                if vr_errors:
                    errors.append(vr_errors)
                if vr_warnings:
                    warnings.append(vr_warnings)
            elif (
                val_rule.__contains__("matchExactlyOne")
                and len(present_manifest_log) != 1
            ):
                vr_errors, vr_warnings = GenerateError.generate_cross_warning(
                    val_rule=val_rule,
                    attribute_name=source_attribute,
                    matching_manifests=present_manifest_log,
                    dmge=dmge,
                )
                if vr_errors:
                    errors.append(vr_errors)
                if vr_warnings:
                    warnings.append(vr_warnings)

        logger.debug(
            f"cross manifest validation time {perf_counter()-t_cross_manifest}"
        )
        return errors, warnings
