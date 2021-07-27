import builtins
from jsonschema import ValidationError
import logging
#import numpy as np
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


class ValidateRules(object):
    '''
    Add additional class documentation.
    '''

    def type_validation(self, val_rule, manifest_col):
        '''
        Purpose:
            Check if values for a given manifest attribue are the same type
            specified in val_rule.
        Input:
            - val_rule: str, Validation rule, specifying input type, either
                'float', 'int', 'num', 'str'
            - manifest_col: pd.core.series.Series, column for a given
                attribute in the manifest
        Returns:
            This function will return errors when the user input value
            does not match schema specifications.
        '''
        def generate_type_error(val_rule, row_num, attribute_value, attribute_name):
            logging.error(
                f"On row {row_num} the attribute {attribute_name} does not contain "
                f"the proper value type {val_rule}."
                )
            error_row = row_num # index row of the manifest where the error presented.
            error_col = attribute_name # Attribute name
            error_message = (f"On row {row_num}, type provided was "
                f"{type(attribute_value)} and was supposed to be {val_rule}".format())
            error_val = f"Type Error"
            return [error_row, error_col, error_message, error_val]

        errors = []
        # num indicates either a float or int.
        if val_rule == 'num':
            for i, value in enumerate(manifest_col):
                if bool(value) and not isinstance(value, (int, float)):
                    errors.append(generate_type_error(val_rule, row_num = str(i+2), 
                        attribute_value = manifest_col[i], attribute_name = manifest_col.name))
                    breakpoint()
        elif val_rule in ['int', 'float', 'str']:            
            for i, value in enumerate(manifest_col):
                if bool(value) and type(value) != getattr(builtins, val_rule):
                    errors.append(generate_type_error(val_rule, row_num = str(i+2), 
                        attribute_value = manifest_col[i], attribute_name = manifest_col.name))
                    breakpoint()
        return errors

    def regex_validation(self, val_rule, manifest_col):
        '''
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
            This function will return errors when the user input value
            does not match schema specifications.
        TODO: 
            move validation to convert step.
        '''
        def generate_regex_error(val_rule, reg_expression, row_num, attribute_value, attribute_name):
            '''
            Log and generate an error if a users input values do not match those as specified by the 
            Schema's indicated regular expression.
            '''

            logging.error(
                f"For the attribute {attribute_name}, on row {row_num}, the string is not properly formatted. "
                f"It should follow the following re.{module_to_call} pattern \"{reg_expression}\"."
                )
            error_row = row_num # index row of the manifest where the error presented.
            error_col = attribute_name # Attribute name
            error_message = f"On row {row_num}, the string is not properly formatted. It should follow the following re.{module_to_call} pattern \"{reg_expression}\"."
            error_val = f"Type Error"
            return [error_row, error_col, error_message, error_val]
        
        reg_exp_rules = val_rule.split(' ')

        try:
            module_to_call = getattr(re, reg_exp_rules[1])
            reg_expression = reg_exp_rules[2]
        except:
            raise ValidationError(
                f"The regex rules were not provided properly for attribute {manifest_col.name}."
                f" They should be provided as follows ['regex', 'module name', 'regular expression']")
       
        errors = []

        # Handle case where validating re's within a list.
        if type(manifest_col[0]) == list:
            for i, row_values in enumerate(manifest_col):
                for j, re_to_check in enumerate(row_values):
                    re_to_check = str(re_to_check)
                    if not bool(module_to_call(reg_expression, re_to_check)) and bool(re_to_check):
                        errors.append(generate_regex_error(val_rule, reg_expression, row_num = str(i+2), 
                                attribute_value = manifest_col[i], attribute_name = manifest_col.name))
        # Validating single re's    
        else:
            manifest_col = manifest_col.astype(str)
            for i, re_to_check in enumerate(manifest_col):
                if not bool(module_to_call(reg_expression, re_to_check)) and bool(re_to_check):
                    errors.append(generate_regex_error(val_rule, reg_expression, row_num = str(i+2), 
                            attribute_value = manifest_col[i], attribute_name = manifest_col.name))
            
        return errors

    def url_validation(self, val_rule, manifest_col):
        '''
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
        '''

        def generate_url_error(url, url_error, row_num, 
                        attribute_name, argument):
            '''
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
                Error details for further storage.
            '''
            error_row = row_num # index row of the manifest where the error presented.
            error_col = attribute_name # Attribute name
            if url_error == 'invalid_url':
                logging.error(
                    f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                    f"conform to the standards of a URL. Please make sure you are entering a real, working URL "
                    f"as required by the Schema."
                    )
                error_message = (f"For the attribute '{attribute_name}', on row {row_num}, "
                    f"the URL provided ({url})does not conform to the standards of a URL. "
                    f"Please make sure you are entering a real, working URL as required by the Schema.")
                error_val = f"URL Error: Invalid URL"
            elif url_error == 'arg_error':
                logging.error(
                    f"For the attribute '{attribute_name}', on row {row_num}, the URL provided ({url}) does not "
                    f"conform to the schema specifications and does not contain the required element: {argument}."
                    )
                error_message = (
                    f"For the attribute {attribute_name}, on row {row_num}, the URL provided ({url}) "
                    f"does not conform to the schema specifications and does not contain the required element: {argument}.")
                error_val = f"URL Error: Argument Error"
            elif url_error == 'random_entry':
                logging.error(
                    f"For the attribute '{attribute_name}', on row {row_num}, the input provided ('{url}'') does not "
                    f"look like a URL, please check input and try again."
                    )
                error_message = (
                    f"For the attribute '{attribute_name}', on row {row_num}, "
                    f"the input provided ('{url}'') does not look like a URL, please check input and try again.")
                error_val = f"URL Error: Random Entry"
            return [error_row, error_col, error_message, error_val]

        url_args = val_rule.split(' ')[1:]
        errors = []

        for i, url in enumerate(manifest_col):
            # Check if a random phrase, string or number was added and 
            # log the appropriate error.
            if not (urlparse(url).scheme + urlparse(url).netloc +  urlparse(url).params +
                urlparse(url).query + urlparse(url).fragment):
                # 
                url_error = 'random_entry'
                valid_url = False
                errors.append(generate_url_error(url, url_error = url_error,
                    row_num = str(+2), attribute_name = manifest_col.name,
                    argument = url_args))
            else:
                # add scheme to the URL if not currently added.
                if not urlparse(url).scheme:
                    url = "http://"+url
                try:
                    # Check that the URL points to a working webpage
                    # if not log the appropriate error.
                    request = Request(url)
                    response = urlopen(request)
                    valid_url = True
                    response_code = response.getcode()
                except:
                    valid_url= False
                    url_error = 'invalid_url'
                    errors.append(generate_url_error(url, url_error = url_error, 
                        row_num = str(i+2), attribute_name = manifest_col.name, 
                        argument = url_args))
                if valid_url == True:
                    # If the URL works, check to see if it contains the proper arguments
                    # as specified in the schema.
                    for arg in url_args:
                        if arg not in url:
                            url_error = 'arg_error'
                            errors.append(generate_url_error(url, 
                                url_error = url_error, 
                                row_num = str(i+2), attribute_name = manifest_col.name, 
                                argument = arg))
        return errors
    def list_validation(self, val_rule, manifest_col):
        '''
        Purpose:
            Determine if values for a particular attribute are comma separated.
        Input:
            - val_rule: str, Validation rule
            - manifest_col: pd.core.series.Series, column for a given attribute
        Returns:
            - string_as_list: Input values re-formatted to a list and 
            - Error log.
        '''
        def generate_list_error(list_string, row_num, attribute_name, list_error):
            '''
            Purpose:
                If an error is found in the string formatting, detect and record
                an error message.
            Input:
                - list_string: the user input list, that is represented as a string.
                - row_num: the row the error occurred on.
                - attribute_name: the attribute the error occurred on.
            Output:
                Error message and log.
            '''
            if list_error == 'not_comma_delimited':
                logging.error(
                    f"For attribute {attribute_name} in row {row_num} it does not "
                    f"appear as if you provided a comma delimited string. Please check "
                    f"your entry ('{list_string}'') and try again."
                    )
                error_row = row_num # index row of the manifest where the error presented.
                error_col = attribute_name # Attribute name
                error_message = (
                    f"For attribute {attribute_name} in row {row_num} it does not "
                    f"appear as if you provided a comma delimited string. Please check "
                    f"your entry ('{list_string}'') and try again."
                    )
                error_val = f"List Error"
            return [error_row, error_col, error_message, error_val]

        # For each 'list' (input as a string with a , delimiter) entered,
        # convert to a real list of strings, with leading and trailing
        # white spaces removed.
        errors = []
        manifest_col = manifest_col.astype(str)
        # This will capture any if an entry is not formatted properly.
        for row_num, list_string in enumerate(manifest_col):
            if ',' not in list_string and bool(list_string):
                list_error = 'not_comma_delimited'
                errors.append(generate_list_error(list_string, row_num = str(row_num+2), 
                    attribute_name = manifest_col.name, list_error= list_error))
        # Convert string to list.
        manifest_col = manifest_col.apply(
            lambda x: [s.strip() for s in str(x).split(",")])

        return errors, manifest_col

