from jsonschema import ValidationError
import logging
import pandas as pd
from typing import Any, Dict, Optional, Text, List


logger = logging.getLogger(__name__)


def get_error(validation_rules: list, 
        attribute_name: str, error_type: str, input_filetype:str,) -> List[str]:
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

    if error_type == 'invalid_rule_combination':
        first_type=validation_rules[0].split(' ')[0]
        second_type=valid_rule_combinations()[first_type]
        error_str = (f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules ({'::'.join(validation_rules)}) are not "
            f"a valid combination of rules. The validation rule [{first_type}] may only be used with rules of type {second_type}.")
        logging.error(error_str)
        error_message = error_str
        error_val = f"Invalid rule combinaion."
        
    return ['NA', error_col, error_message, error_val]

def validate_single_rule(validation_rule, attribute, input_filetype):
    '''
    TODO:reformat validation_types from validate_manifest to document the 
    arguments allowed. Keep in that location and pull into this function.
    '''
    errors = []
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
                val_rule.strip() for val_rule in validation_rule.strip().split(" ")]

    rule_type = validation_rule_with_args[0]

    # Check that the rule is actually a valid rule type.
    if rule_type not in validation_types.keys():
        errors.append(get_error(validation_rule, attribute,
            error_type = 'not_rule', input_filetype=input_filetype))
    else:
        if 'fixed_arg' in validation_types[rule_type].keys():
            fixed_args = validation_types[rule_type]['fixed_arg']
            num_args = len([vr for vr in validation_rule_with_args if vr not in fixed_args])-1 
        else:
            num_args = len(validation_rule_with_args) - 1
        if num_args:
            argument_allowed, num_allowed = validation_types[rule_type]['arguments']
            # If arguments are allowed, check that the correct amount have been passed.
            if argument_allowed:
                # Remove any fixed args from our calc.
                
                # Are limits placed on the number of arguments.
                if num_allowed is not None and num_allowed != num_args:
                    errors.append(get_error(validation_rule, attribute,
                        error_type = 'incorrect_num_args', input_filetype=input_filetype))
            # If arguments are provided but not allowed raise an error.
            else:
                errors.append(get_error(validation_rule, attribute,
                    error_type = 'args_not_allowed', input_filetype=input_filetype))
        if ':' in validation_rule:
            errors.append(get_error(validation_rule, attribute,
                error_type = 'delimiter', input_filetype=input_filetype))
    return errors

def valid_rule_combinations():
    complementary_rules = {
        "int": ['inRange',],
        "float": ['inRange'],
        "num": ['inRange'],
        "protectAges": ['inRange'],
        "inRange": ['int','float','num','protectAges'],
        "list": ['regex'],
        "regex": ['list']
    }

    return complementary_rules


def validate_schema_rules(validation_rules, attribute, input_filetype):
    '''
    validation_rules: list
    input_filetype: str, used in error generation to aid user in
        locating the source of the error.

    Validation Rules Formatting rules:
    Multiple Rules:
        max of 2 rules
        if list and regex list must be first
    Single Rules:
        Additional arg
    '''
    # Specify all the validation types and whether they currently
    # allow users to pass additional arguments (when used on their own), and if there is
    # a set number of arguments required.

    errors = []
    complementary_rules = valid_rule_combinations()

    num_validation_rules = len(validation_rules)

    print(validation_rules)
    # Check for edge case that user has entered more than 2 rules,
    # throw an error if they have.
    if num_validation_rules > 2:
            errors.append(get_error(validation_rules, attribute,
                error_type = 'too_many_rules', input_filetype=input_filetype))

    elif num_validation_rules == 2: 
        first_rule, second_rule = validation_rules
        first_type = first_rule.split(" ")[0]  
        second_type = second_rule.split(" ")[0]  

        # validate rule combination
        if second_type not in complementary_rules[first_type]:
            errors.append(get_error(validation_rules, attribute, 
                error_type = 'invalid_rule_combination', input_filetype=input_filetype))
        
        # validate order for combinations including list
        if 'list' in validation_rules:
            # For multiple rules include list check that the first rule is 'list', if not, throw an error
            if not first_rule == 'list':
                errors.append(get_error(validation_rules, attribute, 
                    error_type = 'list_not_first', input_filetype=input_filetype))
        
    # validate each rule individually in the combo
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