from jsonschema import ValidationError
import logging
import pandas as pd
from typing import Any, Dict, Optional, Text, List


logger = logging.getLogger(__name__)


def get_error(validation_rules: list, 
        attribute_name: str, error_type: str, input_filetype:str) -> List[str]:
    '''
    Generate error message for errors when trying to specify 
    multiple validation rules.
    '''
    error_col = attribute_name # Attribute name
    if error_type == 'too_many_rules':
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) ."
        f"have too many entries. We currently only specify two rules ('list :: another_rule').")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Multiple Rules: too many rules"
    
    if error_type == 'list_not_first':
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) are improperly "
            f"specified. 'list' must be first.")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Multiple Rules: list not first"
    
    if error_type == 'second_rule':
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) are improperly "
            f"specified. 'regex' must be the second rule.")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Multiple Rules: Second rule needs to be regex"
    
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
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({validation_rules}) is not "
            f"formatted properly. The number of provided arguments does not match the number required/allowed.")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Incorrect num arguments."
    return ['NA', error_col, error_message, error_val]

def validate_single_rule(validation_rules, errors, attribute, input_filetype):
    '''
    TODO:reformat validation_types from validate_manifest to document the 
    arguments allowed. Keep in that location and pull into this function.
    '''
    validation_types = {
            "int": {'arguments':(False, None)},
            "float": {'arguments':(False, None)},
            "num": {'arguments':(False, None)},
            "str": {'arguments':(False, None)},
            "regex": {'arguments':(True, 2), 'fixed_arg': ['strict']},
            "url" : {'arguments':(True, None)},
            "list": {'arguments':(False, None)},
            "matchAtLeastOne": {'arguments':(True, 2)},
            "matchExactlyOne": {'arguments':(True, 2)},
            "recommended": {'arguments':(False, None)},
            "protectAges": {'arguments':(True, 1)},
            "unique": {'arguments':(True, 1)},
            "inRange": {'arguments':(True, 3)},
            }
    validation_rule_with_args = [
                val_rule.strip() for val_rule in validation_rules.strip().split(" ")]

    # Check that the rule is actually a valid rule type.
    if validation_rule_with_args[0] not in validation_types.keys():
        errors.append(get_error(validation_rules, attribute,
            error_type = 'not_rule', input_filetype=input_filetype))
    else:
        if 'fixed_arg' in validation_types[validation_rule_with_args[0]].keys():
            fixed_args = validation_types[validation_rule_with_args[0]]['fixed_arg']
            num_args = len([vr for vr in validation_rule_with_args if vr not in fixed_args])-1 
        else:
            num_args = len(validation_rule_with_args) - 1
        if num_args:
            argument_allowed, num_allowed = validation_types[validation_rule_with_args[0]]['arguments']
            # If arguments are allowed, check that the correct amount have been passed.
            if argument_allowed:
                # Remove any fixed args from our calc.
                
                # Are limits placed on the number of arguments.
                if num_allowed is not None and num_allowed != num_args:
                    errors.append(get_error(validation_rules, attribute,
                        error_type = 'incorrect_num_args', input_filetype=input_filetype))
            # If arguments are provided but not allowed raise an error.
            else:
                errors.append(get_error(validation_rules, attribute,
                    error_type = 'args_not_allowed', input_filetype=input_filetype))
    return errors

def validate_schema_rules(validation_rules, attribute, input_filetype):
    '''
    validation_rules: list
    input_filetype: str, used in error generation to aid user in
        locating the source of the error.

    Validation Rules Formatting rules:
    Multiple Rules:
        Allowed for list :: regex only
    Single Rules:
        Additional arg
    '''
     
    # Specify all the validation types and whether they currently
    # allow users to pass additional arguments (when used on their own), and if there is
    # a set number of arguments required.

    errors = []

    num_validation_rules = len(validation_rules)

    # Validate for multiple rules
    if num_validation_rules == 2:
        # For multiple rules check that the first rule listed is 'list'
        # if not, throw an error (this is the only format currently supported).
        if not validation_rules[0] == 'list':
            errors.append(get_error(validation_rules, attribute, 
                error_type = 'list_not_first', input_filetype=input_filetype))
        elif (validation_rules[0] == 'list'):
            second_rule = validation_rules[1].split(' ')
            second_type = second_rule[0]
            # for now we are only supporting multiple rules as 
            # list and regex. Check that this is the case.
            if second_type == 'regex':
                errors.extend(validate_single_rule(validation_rules[1], errors, 
                    attribute, input_filetype))
            elif second_type != 'regex':
                if ':' in second_type[-1]:
                    errors.append(get_error(validation_rules, attribute,
                        error_type = 'delimiter', input_filetype=input_filetype))
                else:
                    errors.append(get_error(validation_rules, attribute,
                        error_type = 'second_rule', input_filetype=input_filetype))
                
    # Check for edge case that user has entered more than 2 rules,
    # throw an error if they have.
    elif num_validation_rules > 2:
            errors.append(get_error(validation_rules, attribute,
                error_type = 'too_many_rules', input_filetype=input_filetype))
    elif num_validation_rules == 1:
        errors.extend(validate_single_rule(validation_rules[0], errors,
            attribute, input_filetype))

            #breakpoint()
    if errors:
        raise ValidationError(
                        f"The {input_filetype} has an error in the validation_rules set "
                        f"for attribute {attribute}. "
                        f"Validation failed with the following errors: {errors}"
                    )
    return 