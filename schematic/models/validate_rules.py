import builtins
from jsonschema import ValidationError
import logging
#import numpy as np
import pandas as pd
import re
import sys
# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text, List

logger = logging.getLogger(__name__)


class ValidateRules(object):
    
    def __init__(
            self,
            validation_rule: str,
            manifest_col: pd.core.series.Series,
    ) -> None:
                   
        validation_types = {"int": "type_validation",
                            "float": "type_validation",
                            "num": "type_validation",
                            "str": "type_validation",
                            "regex": "regex_validation",
                            "url" : "url_validation"
                            }
    
        self.validation_type = validation_rule.split(' ')[0]
        self.validation_method = getattr(ValidateRules, validation_types[self.validation_type])
        self.validation_method(self, validation_rule, manifest_col)

    
        
    def type_validation(self, val_rule, manifest_col):
        '''
        Note: Keeping num and float/int separated to allow for more fine grained type validation 
        in the future.

        '''
        def generate_type_error(val_rule, row_num, attribute_value, attribute_name):
            try:
                logging.error(
                    f"On row {row_num} the attribute {attribute_name} does not contain "
                    f"the proper value type {val_rule}."
                    )
                error_row = row_num # index row of the manifest where the error presented.
                error_col = attribute_name # Attribute name
                error_message = f"On row {row_num}, type provided was {type(attribute_value)} and was supposed to be {val_rule}".format()
                error_val = f"Type Error"
            except:
                breakpoint()
            return [error_row, error_col, error_message, error_val]

        errors = []
        # if a value is listed to be a num and is not showing up as either a float or int.
        if val_rule == 'num':
            for i, value in enumerate(manifest_col):
                if bool(value) and not isinstance(value, (int, float)):
                    errors.append(generate_type_error(val_rule, row_num = str(i), 
                        attribute_value = manifest_col[i], attribute_name = manifest_col.name))
                    breakpoint()
        elif val_rule in ['int', 'float', 'str']:            
            for i, value in enumerate(manifest_col):
                if bool(value) and type(value) != getattr(builtins, val_rule):
                    errors.append(generate_type_error(val_rule, row_num = str(i), 
                        attribute_value = manifest_col[i], attribute_name = manifest_col.name))
                    breakpoint()
        return errors

    def regex_validation(self, val_rule, manifest_col):
        '''

        '''
        def generate_regex_error(val_rule, reg_expression, row_num, attribute_value, attribute_name):
            try:
                logging.error(
                    f"For the attribute {attribute_name}, on row {row_num}, the string is not properly formatted. "
                    f"It should follow the following re.{module_to_call} pattern \"{reg_expression}\"."
                    )
                error_row = row_num # index row of the manifest where the error presented.
                error_col = attribute_name # Attribute name
                error_message = f"On row {row_num}, the string is not properly formatted. It should follow the following re.{module_to_call} pattern \"{reg_expression}\"."
                error_val = f"Type Error"
            except:
                breakpoint()
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
        manifest_col = manifest_col.astype(str)
        for i, re_to_check in enumerate(manifest_col):
            if not bool(module_to_call(reg_expression, re_to_check)):
                errors.append(generate_regex_error(val_rule, reg_expression, row_num = str(i), 
                        attribute_value = manifest_col[i], attribute_name = manifest_col.name))
        
        return errors

    def url_validation(self, val_rule, manifest_col):
        '''
        Schema can require various URL's.
        Want to validate the URL's are following schema rules and are not leading 
        to a broken site.
        '''
        errors = []

        return errors

    
    '''
    # convert manifest values to string
    # TODO: when validation handles annotation types as validation rules
    # would have to avoid converting everything to string
    manifest[col] = manifest[col].astype(str)

    # List Validation

    # if the validation rule is set to list, convert items in the
    # annotations manifest to a list and strip each value from leading/trailing spaces
    # Note this is not a real 'rule' and will not throw an error.

    if "list" in col_val_rules:
        manifest[col] = manifest[col].apply(
            lambda x: [s.strip() for s in str(x).split(",")]
        )
    '''