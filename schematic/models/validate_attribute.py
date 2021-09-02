import builtins
from jsonschema import ValidationError
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

logger = logging.getLogger(__name__)


class GenerateError:
    def generate_list_error(
        list_string: str, row_num: str, attribute_name: str, list_error: str
    ) -> List[str]:
        """
            Purpose:
                If an error is found in the string formatting, detect and record
                an error message.
            Input:
                - list_string: the user input list, that is represented as a string.
                - row_num: the row the error occurred on.
                - attribute_name: the attribute the error occurred on.
            Returns:
                Logging.error.
                Errors: List[str] Error details for further storage.
            """
        if list_error == "not_comma_delimited":
            error_str = (
                f"For attribute {attribute_name} in row {row_num} it does not "
                f"appear as if you provided a comma delimited string. Please check "
                f"your entry ('{list_string}'') and try again."
            )
            logging.error(error_str)
            error_row = row_num  # index row of the manifest where the error presented.
            error_col = attribute_name  # Attribute name
            error_message = error_str
            error_val = f"List Error"
        return [error_row, error_col, error_message, error_val]

    def generate_regex_error(
        val_rule: str,
        reg_expression: str,
        row_num: str,
        module_to_call: str,
        attribute_name: str,
    ) -> List[str]:
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
            Returns:
                Logging.error.
                Errors: List[str] Error details for further storage.
            """
        regex_error_string = (
            f"For the attribute {attribute_name}, on row {row_num}, the string is not properly formatted. "
            f'It should follow the following re.{module_to_call} pattern "{reg_expression}".'
        )
        logging.error(regex_error_string)
        error_row = row_num  # index row of the manifest where the error presented.
        error_col = attribute_name  # Attribute name
        error_message = regex_error_string
        error_val = f"Type Error"
        return [error_row, error_col, error_message, error_val]

    def generate_type_error(
        val_rule: str, row_num: str, attribute_name: str
    ) -> List[str]:
        """
            Purpose:
                Generate an logging error as well as a stored error message, when
                a type error is encountered.
            Input:
                val_rule: str, defined in the schema.
                row_num: str, row where the error was detected
                attribute_name: str, attribute being validated
            Returns:
                Logging.error.
                Errors: List[str] Error details for further storage.
            """
        type_error_str = (
            f"On row {row_num} the attribute {attribute_name} "
            f"does not contain the proper value type {val_rule}."
        )
        logging.error(type_error_str)
        error_row = row_num  # index row of the manifest where the error presented.
        error_col = attribute_name  # Attribute name
        error_message = type_error_str
        error_val = f"Type Error"
        return [error_row, error_col, error_message, error_val]

    def generate_url_error(
        url: str, url_error: str, row_num: str, attribute_name: str, argument: str
    ) -> List[str]:
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
                attribute_name: str, attribute being validated
                argument: str, argument being validated.
            Returns:
                Logging.error.
                Errors: List[str] Error details for further storage.
            """
        error_row = row_num  # index row of the manifest where the error presented.
        error_col = attribute_name  # Attribute name
        if url_error == "invalid_url":
            invalid_url_error_string = (
                f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                f"conform to the standards of a URL. Please make sure you are entering a real, working URL "
                f"as required by the Schema."
            )
            logging.error(invalid_url_error_string)
            error_message = invalid_url_error_string
            error_val = f"URL Error: Invalid URL"
        elif url_error == "arg_error":
            arg_error_string = (
                f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                f"conform to the schema specifications and does not contain the required element: {argument}."
            )
            logging.error(arg_error_string)
            error_message = arg_error_string
            error_val = f"URL Error: Argument Error"
        elif url_error == "random_entry":
            random_entry_error_str = (
                f"For the attribute '{attribute_name}', on row {row_num}, the input provided ('{url}'') does not "
                f"look like a URL, please check input and try again."
            )
            logging.error(random_entry_error_str)
            error_message = random_entry_error_str
            error_val = f"URL Error: Random Entry"
        return [error_row, error_col, error_message, error_val]


class ValidateAttribute(object):
    """
    A collection of functions to validate manifest attributes.
        list_validation
        regex_validation
        type_validation
        url_validation
    See functions for more details.
    TODO:
        - Add year validator
        - Add string length validator
    """

    def list_validation(
        self, val_rule: str, manifest_col: pd.core.series.Series
    ) -> (List[List[str]], pd.core.frame.DataFrame):
        """
        Purpose:
            Determine if values for a particular attribute are comma separated.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given attribute
        Returns:
            - manifest_col: Input values in manifest arere-formatted to a list
            - Error log, error list
        """

        # For each 'list' (input as a string with a , delimiter) entered,
        # convert to a real list of strings, with leading and trailing
        # white spaces removed.
        errors = []
        manifest_col = manifest_col.astype(str)
        # This will capture any if an entry is not formatted properly.
        for row_num, list_string in enumerate(manifest_col):
            if "," not in list_string and bool(list_string):
                list_error = "not_comma_delimited"
                errors.append(
                    GenerateError.generate_list_error(
                        list_string,
                        row_num=str(row_num + 2),
                        attribute_name=manifest_col.name,
                        list_error=list_error,
                    )
                )
        # Convert string to list.
        manifest_col = manifest_col.apply(
            lambda x: [s.strip() for s in str(x).split(",")]
        )

        return errors, manifest_col

    def regex_validation(
        self, val_rule: str, manifest_col: pd.core.series.Series
    ) -> List[List[str]]:
        """
        Purpose:
            Check if values for a given manifest attribue conform to the reguar expression,
            provided in val_rule.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given
                attribute in the manifest
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
            Logging.error.
            Errors: List[str] Error details for further storage.
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

        # Handle case where validating re's within a list.
        if type(manifest_col[0]) == list:
            for i, row_values in enumerate(manifest_col):
                for j, re_to_check in enumerate(row_values):
                    re_to_check = str(re_to_check)
                    if not bool(module_to_call(reg_expression, re_to_check)) and bool(
                        re_to_check
                    ):
                        errors.append(
                            GenerateError.generate_regex_error(
                                val_rule,
                                reg_expression,
                                row_num=str(i + 2),
                                module_to_call=reg_exp_rules[1],
                                attribute_name=manifest_col.name,
                            )
                        )
        # Validating single re's
        else:
            manifest_col = manifest_col.astype(str)
            for i, re_to_check in enumerate(manifest_col):
                if not bool(module_to_call(reg_expression, re_to_check)) and bool(
                    re_to_check
                ):
                    errors.append(
                        GenerateError.generate_regex_error(
                            val_rule,
                            reg_expression,
                            row_num=str(i + 2),
                            module_to_call=reg_exp_rules[1],
                            attribute_name=manifest_col.name,
                        )
                    )

        return errors

    def type_validation(
        self, val_rule: str, manifest_col: pd.core.series.Series
    ) -> List[List[str]]:
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
            Logging.error.
            Errors: List[str] Error details for further storage.
        TODO:
            Convert all inputs to .lower() just to prevent any entry errors.
        """

        errors = []
        # num indicates either a float or int.
        if val_rule == "num":
            for i, value in enumerate(manifest_col):
                if bool(value) and not isinstance(value, (int, float)):
                    errors.append(
                        GenerateError.generate_type_error(
                            val_rule,
                            row_num=str(i + 2),
                            attribute_name=manifest_col.name,
                        )
                    )
        elif val_rule in ["int", "float", "str"]:
            for i, value in enumerate(manifest_col):
                if bool(value) and type(value) != getattr(builtins, val_rule):
                    errors.append(
                        GenerateError.generate_type_error(
                            val_rule,
                            row_num=str(i + 2),
                            attribute_name=manifest_col.name,
                        )
                    )
        return errors

    def url_validation(self, val_rule: str, manifest_col: str) -> List[List[str]]:
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

        for i, url in enumerate(manifest_col):
            # Check if a random phrase, string or number was added and
            # log the appropriate error.
            if not (
                urlparse(url).scheme
                + urlparse(url).netloc
                + urlparse(url).params
                + urlparse(url).query
                + urlparse(url).fragment
            ):
                #
                url_error = "random_entry"
                valid_url = False
                errors.append(
                    GenerateError.generate_url_error(
                        url,
                        url_error=url_error,
                        row_num=str(i + 2),
                        attribute_name=manifest_col.name,
                        argument=url_args,
                    )
                )
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
                    errors.append(
                        GenerateError.generate_url_error(
                            url,
                            url_error=url_error,
                            row_num=str(i + 2),
                            attribute_name=manifest_col.name,
                            argument=url_args,
                        )
                    )
                if valid_url == True:
                    # If the URL works, check to see if it contains the proper arguments
                    # as specified in the schema.
                    for arg in url_args:
                        if arg not in url:
                            url_error = "arg_error"
                            errors.append(
                                GenerateError.generate_url_error(
                                    url,
                                    url_error=url_error,
                                    row_num=str(i + 2),
                                    attribute_name=manifest_col.name,
                                    argument=arg,
                                )
                            )
        return errors
