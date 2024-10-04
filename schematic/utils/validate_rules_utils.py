"""validate rules utils"""

import logging
from typing import Literal, Optional, TypedDict

from jsonschema import ValidationError
from typing_extensions import assert_never

logger = logging.getLogger(__name__)


class Rule(TypedDict):
    """A validation rule"""

    arguments: tuple[int, int]
    type: str
    complementary_rules: Optional[list[str]]
    default_message_level: Optional[str]
    fixed_arg: Optional[list[str]]


def validation_rule_info() -> dict[str, Rule]:
    """
    Function to return dict that holds information about each rule
    Will be pulled into validate_single_rule, validate_manifest_rules, validate_schema_rules
    Structure:
        Rule:{
            'arguments':(<num arguments allowed>, <num arguments required>),
            'type': <rule type>,
            'complementary_rules': [<rules available for pairing>]}
        }
    """
    return {
        "int": {
            "arguments": (1, 0),
            "type": "type_validation",
            "complementary_rules": ["inRange", "IsNA"],
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "float": {
            "arguments": (1, 0),
            "type": "type_validation",
            "complementary_rules": ["inRange", "IsNA"],
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "num": {
            "arguments": (1, 0),
            "type": "type_validation",
            "complementary_rules": ["inRange", "IsNA"],
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "str": {
            "arguments": (1, 0),
            "type": "type_validation",
            "complementary_rules": None,
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "date": {
            "arguments": (1, 0),
            "type": "content_validation",
            "complementary_rules": None,
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "regex": {
            "arguments": (3, 2),
            "fixed_arg": ["strict"],
            "type": "regex_validation",
            "complementary_rules": ["list"],
            "default_message_level": "error",
        },
        "url": {
            "arguments": (101, 0),
            "type": "url_validation",
            "complementary_rules": None,
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "list": {
            "arguments": (2, 0),
            "type": "list_validation",
            "complementary_rules": ["regex"],
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "matchAtLeastOne": {
            "arguments": (3, 2),
            "type": "cross_validation",
            "complementary_rules": None,
            "default_message_level": "warning",
            "fixed_arg": None,
        },
        "matchExactlyOne": {
            "arguments": (3, 2),
            "type": "cross_validation",
            "complementary_rules": None,
            "default_message_level": "warning",
            "fixed_arg": None,
        },
        "matchNone": {
            "arguments": (3, 2),
            "type": "cross_validation",
            "complementary_rules": None,
            "default_message_level": "warning",
            "fixed_arg": None,
        },
        "recommended": {
            "arguments": (1, 0),
            "type": "content_validation",
            "complementary_rules": None,
            "default_message_level": "warning",
            "fixed_arg": None,
        },
        "protectAges": {
            "arguments": (1, 0),
            "type": "content_validation",
            "complementary_rules": [
                "inRange",
            ],
            "default_message_level": "warning",
            "fixed_arg": None,
        },
        "unique": {
            "arguments": (1, 0),
            "type": "content_validation",
            "complementary_rules": None,
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "inRange": {
            "arguments": (3, 2),
            "type": "content_validation",
            "complementary_rules": ["int", "float", "num", "protectAges"],
            "default_message_level": "error",
            "fixed_arg": None,
        },
        "IsNA": {
            "arguments": (0, 0),
            "type": "content_validation",
            "complementary_rules": [
                "int",
                "float",
                "num",
            ],
            "default_message_level": None,
            "fixed_arg": None,
        },
        "filenameExists": {
            "arguments": (1, 1),
            "type": "filename_validation",
            "complementary_rules": None,
            "default_message_level": "error",
            "fixed_arg": None,
        },
    }


def get_error(
    validation_rules: str,
    attribute_name: str,
    error_type: Literal[
        "delimiter", "not_rule", "args_not_allowed", "incorrect_num_args"
    ],
    input_filetype: str,
) -> list[str]:
    """
    Generate error message for errors when trying to specify
    multiple validation rules.
    """
    error_col = attribute_name  # Attribute name

    if error_type == "delimiter":
        error_str = (
            f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules "
            f"({validation_rules}) are improperly "
            "specified. Please check your delimiter is '::'"
        )
        logging.error(error_str)
        error_message = error_str
        error_val = "Multiple Rules: Delimiter"

    elif error_type == "not_rule":
        error_str = (
            f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules "
            f"({validation_rules}) is not "
            "a valid rule. Please check spelling."
        )
        logging.error(error_str)
        error_message = error_str
        error_val = "Not a Rule"

    elif error_type == "args_not_allowed":
        error_str = (
            f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules "
            f"({validation_rules}) is not"
            "formatted properly. No additional arguments are allowed for this rule."
        )
        logging.error(error_str)
        error_message = error_str
        error_val = "Args not allowed."

    elif error_type == "incorrect_num_args":
        rule_type = validation_rules.split(" ")[0]

        if rule_type in validation_rule_info():
            arg_tuple = validation_rule_info()[rule_type]["arguments"]
            assert isinstance(arg_tuple, tuple)
            assert len(arg_tuple) == 2
            number_allowed = str(arg_tuple[0])
            number_required = str(arg_tuple[1])
        else:
            number_allowed, number_required = ("", "")

        error_str = (
            f"The {input_filetype}, has an error in the validation rule "
            f"for the attribute: {attribute_name}, the provided validation rules "
            f"({validation_rules}) is not "
            "formatted properly. The number of provided arguments does not match the "
            f"number allowed({number_allowed}) or required({number_required})."
        )
        logging.error(error_str)
        error_message = error_str
        error_val = "Incorrect num arguments."

    else:
        assert_never(error_type)

    return ["NA", error_col, error_message, error_val]


def validate_single_rule(
    validation_rule: str, attribute: str, input_filetype: str
) -> list[list[str]]:
    """
    Perform validation for a single rule to ensure it is specified
      correctly with an appropriate number of arguments
    Inputs:
        validation_rule: single rule being validated
        attribute: attribute validation rule was specified for
        input_filetype: filetype of model input

    Returns:
        errors: List of errors
    """
    errors: list[list[str]] = []
    validation_types = validation_rule_info()
    validation_rule_with_args = [
        val_rule.strip() for val_rule in validation_rule.strip().split(" ")
    ]

    rule_type = validation_rule_with_args[0]

    # ensure rules are not delimited incorrectly
    if ":" in validation_rule:
        errors.append(
            get_error(
                validation_rule,
                attribute,
                error_type="delimiter",
                input_filetype=input_filetype,
            )
        )
    # Check that the rule is actually a valid rule type.
    elif rule_type not in validation_types:
        errors.append(
            get_error(
                validation_rule,
                attribute,
                error_type="not_rule",
                input_filetype=input_filetype,
            )
        )
    # if the rule is indeed a rule and formatted correctly, check that arguments are appropriate
    else:
        arg_tuple = validation_rule_info()[rule_type]["arguments"]
        assert isinstance(arg_tuple, tuple)
        assert len(arg_tuple) == 2
        arguments_allowed, arguments_required = arg_tuple
        # Remove any fixed args from our calc.
        fixed_args = validation_types[rule_type]["fixed_arg"]
        if fixed_args:
            num_args = (
                len([vr for vr in validation_rule_with_args if vr not in fixed_args])
                - 1
            )
        else:
            num_args = len(validation_rule_with_args) - 1

        # If arguments are provided but not allowed, raise an error.
        if num_args and not arguments_allowed:
            errors.append(
                get_error(
                    validation_rule,
                    attribute,
                    error_type="args_not_allowed",
                    input_filetype=input_filetype,
                )
            )

        # If arguments are allowed, check that the correct amount have been passed.
        # There must be at least the number of args required,
        # and not more than allowed
        elif arguments_allowed:
            if (num_args < arguments_required) or (num_args > arguments_allowed):
                errors.append(
                    get_error(
                        validation_rule,
                        attribute,
                        error_type="incorrect_num_args",
                        input_filetype=input_filetype,
                    )
                )

    return errors


def validate_schema_rules(
    validation_rules: list[str], attribute: str, input_filetype: str
) -> None:
    """
    validation_rules: list
    input_filetype: str, used in error generation to aid user in
        locating the source of the error.

    Validation Rules Formatting rules:
    Single Rules:
        Specified with the correct required arguments with no more than what is allowed
    """
    errors = []

    # validate each individual rule
    for rule in validation_rules:
        errors.extend(validate_single_rule(rule, attribute, input_filetype))

    if errors:
        raise ValidationError(
            f"The {input_filetype} has an error in the validation_rules set "
            f"for attribute {attribute}. "
            f"Validation failed with the following errors: {errors}"
        )
