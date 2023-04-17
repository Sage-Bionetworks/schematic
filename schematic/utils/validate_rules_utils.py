from ast import arg
from jsonschema import ValidationError
import logging
import pandas as pd
from typing import Any, Dict, Optional, Text, List


logger = logging.getLogger(__name__)

def validation_rule_info():
    '''
    Function to return dict that holds information about each rule
    Will be pulled into validate_single_rule, validate_manifest_rules, validate_schema_rules
    Structure:    
        Rule:{
            'arguments':(<num arguments allowed>, <num arguments required>),
            'type': <rule type>,
            'complementary_rules': [<rules available for pairing>]}
        }
    '''
    rule_dict = {
            "int": {
                'arguments':(1, 0),
                'type': "type_validation",
                'complementary_rules': ['inRange', 'IsNA'],
                'default_message_level': 'error'},

            "float": {
                'arguments':(1, 0),
                'type': "type_validation",
                'complementary_rules': ['inRange', 'IsNA'],
                'default_message_level': 'error'},

            "num": {
                'arguments':(1, 0),
                'type': "type_validation",
                'complementary_rules': ['inRange', 'IsNA'],
                'default_message_level': 'error'},

            "str": {
                'arguments':(1, 0),
                'type': "type_validation",
                'complementary_rules': None,
                'default_message_level': 'error'},

            "date": {
                'arguments':(1, 0),
                'type': "content_validation",
                'complementary_rules': None,
                'default_message_level': 'error'
            },

            "regex": {
                'arguments':(3, 2), 
                'fixed_arg': ['strict'], 
                'type': "regex_validation",
                'complementary_rules': ['list'],
                'default_message_level': 'error'},

            "url" : {
                'arguments':(101, 0), 
                'type': "url_validation",
                'complementary_rules': None,
                'default_message_level': 'error'},

            "list": {
                'arguments':(2, 0), 
                'type': "list_validation",
                'complementary_rules': ['regex'],
                'default_message_level': 'error'},
                
            "matchAtLeastOne": {
                'arguments':(3, 2), 
                'type': "cross_validation",
                'complementary_rules': None,
                'default_message_level': 'warning'},

            "matchExactlyOne": {
                'arguments':(3, 2), 
                'type': "cross_validation",
                'complementary_rules': None,
                'default_message_level': 'warning'},
                
            "recommended": {
                'arguments':(1, 0), 
                'type': "content_validation",
                'complementary_rules': None,
                'default_message_level': 'warning'},

            "protectAges": {
                'arguments':(1, 0), 
                'type': "content_validation",
                'complementary_rules': ['inRange',],
                'default_message_level': 'warning'},

            "unique": {
                'arguments':(1, 0), 
                'type': "content_validation",
                'complementary_rules': None,
                'default_message_level': 'error'},
                
            "inRange": {
                'arguments':(3, 2), 
                'type': "content_validation",
                'complementary_rules': ['int','float','num','protectAges'],
                'default_message_level': 'error'},
            
            "IsNA":     {
                'arguments':(1, 0), 
                'type': "content_validation",
                'complementary_rules': ['int', 'float', 'num', ],
                'default_message_level': 'warning'},            
            }

    return rule_dict

def get_error(validation_rules: list, 
        attribute_name: str, error_type: str, input_filetype:str,) -> List[str]:
    '''
    Generate error message for errors when trying to specify 
    multiple validation rules.
    '''
    error_col = attribute_name # Attribute name
    
    if error_type == 'delimiter':
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) are improperly "
            f"specified. Please check your delimiter is '::'")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Multiple Rules: Delimiter"
    
    if error_type == 'not_rule':
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) is not "
            f"a valid rule. Please check spelling.")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Not a Rule"
    
    if error_type == 'args_not_allowed':
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) is not"
            f"formatted properly. No additional arguments are allowed for this rule.")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Args not allowed."
    if error_type == 'incorrect_num_args':
        rule_type=validation_rules.split(" ")[0]
        
        if rule_type in validation_rule_info():
            no_allowed, no_required = validation_rule_info()[rule_type]['arguments']
        else:
            no_allowed, no_required = ('', '')

        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) is not "
            f"formatted properly. The number of provided arguments does not match the number allowed({no_allowed}) or required({no_required}).")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Incorrect num arguments."
        
    return ['NA', error_col, error_message, error_val]

def validate_single_rule(validation_rule, attribute, input_filetype):
    '''
    Perform validation for a single rule to ensure it is specified correctly with an appropriate number of arguments
    Inputs:
        validation_rule: single rule being validated
        attribute: attribute validation rule was specified for
        input_filetype: filetype of model input

    Returns:
        errors: List of errors
    '''
    errors = []
    validation_types = validation_rule_info()
    validation_rule_with_args = [
                val_rule.strip() for val_rule in validation_rule.strip().split(" ")]

    rule_type = validation_rule_with_args[0]

    # ensure rules are not delimited incorrectly
    if ':' in validation_rule:
        errors.append(get_error(validation_rule, attribute,
            error_type = 'delimiter', input_filetype=input_filetype))
    # Check that the rule is actually a valid rule type.
    elif rule_type not in validation_types.keys():
        errors.append(get_error(validation_rule, attribute,
            error_type = 'not_rule', input_filetype=input_filetype))
    # if the rule is indeed a rule and formatted correctly, check that arguments are appropriate
    else:
        arguments_allowed, arguments_required = validation_types[rule_type]['arguments']
        # Remove any fixed args from our calc.
        if 'fixed_arg' in validation_types[rule_type].keys():
            fixed_args = validation_types[rule_type]['fixed_arg']
            num_args = len([vr for vr in validation_rule_with_args if vr not in fixed_args])-1 
        else:
            num_args = len(validation_rule_with_args) - 1
            
        # If arguments are provided but not allowed, raise an error.
        if num_args and not arguments_allowed:
            errors.append(get_error(validation_rule, attribute,
                error_type = 'args_not_allowed', input_filetype=input_filetype))
        
        # If arguments are allowed, check that the correct amount have been passed.
        # There must be at least the number of args required,
        # and not more than allowed
        elif arguments_allowed:
            if (num_args < arguments_required) or (num_args > arguments_allowed):
                errors.append(get_error(validation_rule, attribute,
                    error_type = 'incorrect_num_args', input_filetype=input_filetype))

    return errors

def validate_schema_rules(validation_rules, attribute, input_filetype):
    '''
    validation_rules: list
    input_filetype: str, used in error generation to aid user in
        locating the source of the error.

    Validation Rules Formatting rules:
    Single Rules:
        Specified with the correct required arguments with no more than what is allowed
    '''
    errors = []
        
    # validate each individual rule
    for rule in validation_rules:
        errors.extend(validate_single_rule(rule,
            attribute, input_filetype))                

    if errors:
        raise ValidationError(
                        f"The {input_filetype} has an error in the validation_rules set "
                        f"for attribute {attribute}. "
                        f"Validation failed with the following errors: {errors}"
                    )
    
    return 