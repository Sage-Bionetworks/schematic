"""
This module contains functions for interacting with validation rules for JSON Schema creation

JSON Schema docs:

Array: https://json-schema.org/understanding-json-schema/reference/array
Types: https://json-schema.org/understanding-json-schema/reference/type#type-specific-keywords
Format: https://json-schema.org/understanding-json-schema/reference/type#format
Pattern: https://json-schema.org/understanding-json-schema/reference/string#regexp
Min/max: https://json-schema.org/understanding-json-schema/reference/numeric#range

"""
import warnings
from dataclasses import dataclass
from typing import Optional
from schematic.schemas.constants import JSONSchemaType, ValidationRuleName, RegexModule


@dataclass
class ValidationRule:
    """
    This class represents a Schematic validation rule to be used for creating JSON Schemas

    Attributes:
        name: The name of the validation rule
        js_type: The JSON Schema type this rule indicates.
          For example type rules map over to their equivalent JSON Schema type: str -> string
          Other rules have an implicit type. For example the regex rule maps to the JSON
            Schema pattern keyword. The pattern keyword requires the type to be string
        incompatible_rules: Other validation rules this rule can not be paired with
        parameters: Parameters for the validation rule that need to be collected for the JSON Schema
    """

    name: ValidationRuleName
    js_type: Optional[JSONSchemaType]
    incompatible_rules: list[ValidationRuleName]
    parameters: Optional[list[str]] = None


# A dictionary of current Schematic validation rules
#   where the keys are name of the rule in Schematic
#   and the values are ValidationRule objects
_VALIDATION_RULES = {
    "list": ValidationRule(
        name=ValidationRuleName.LIST,
        js_type=None,
        incompatible_rules=[],
    ),
    "date": ValidationRule(
        name=ValidationRuleName.DATE,
        js_type=JSONSchemaType.STRING,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.URL,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
        ],
    ),
    "url": ValidationRule(
        name=ValidationRuleName.URL,
        js_type=JSONSchemaType.STRING,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.DATE,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
        ],
    ),
    "regex": ValidationRule(
        name=ValidationRuleName.REGEX,
        js_type=JSONSchemaType.STRING,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
        ],
        parameters=["module", "pattern"],
    ),
    "inRange": ValidationRule(
        name=ValidationRuleName.IN_RANGE,
        js_type=JSONSchemaType.NUMBER,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
        ],
        parameters=["minimum", "maximum"],
    ),
    "str": ValidationRule(
        name=ValidationRuleName.STR,
        js_type=JSONSchemaType.STRING,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.NUM,
            ValidationRuleName.BOOL,
        ],
    ),
    "float": ValidationRule(
        name=ValidationRuleName.FLOAT,
        js_type=JSONSchemaType.NUMBER,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
            ValidationRuleName.INT,
            ValidationRuleName.NUM,
        ],
    ),
    "int": ValidationRule(
        name=ValidationRuleName.INT,
        js_type=JSONSchemaType.INTEGER,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
            ValidationRuleName.FLOAT,
        ],
    ),
    "num": ValidationRule(
        name=ValidationRuleName.NUM,
        js_type=JSONSchemaType.NUMBER,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
        ],
    ),
}


def filter_unused_inputted_rules(inputted_rules: list[str]) -> list[str]:
    """Filters a list of validation rules for only those used to create JSON Schemas

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        warning: When any of the inputted rules are not used for JSON Schema creation

    Returns:
        A filtered list of validation rules
    """
    unused_rules = [
        rule
        for rule in inputted_rules
        if _get_name_from_inputted_rule(rule)
        not in [e.value for e in ValidationRuleName]
    ]
    if unused_rules:
        msg = f"These validation rules will be ignored in creating the JSON Schema: {unused_rules}"
        warnings.warn(msg)

    return [
        rule
        for rule in inputted_rules
        if _get_name_from_inputted_rule(rule) in [e.value for e in ValidationRuleName]
    ]


def check_for_duplicate_inputted_rules(inputted_rules: list[str]) -> None:
    """Checks that there are no rules with duplicate names

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If there are multiple rules with the same name
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    if sorted(rule_names) != sorted(list(set(rule_names))):
        raise ValueError(f"Validation Rules contains duplicates: {inputted_rules}")


def check_for_conflicting_inputted_rules(inputted_rules: list[str]) -> None:
    """Checks that each rule has no conflicts with any other rule

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If a rule is in conflict with any other rule
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    rules: list[ValidationRule] = _get_rules_by_names(rule_names)
    for rule in rules:
        incompatible_rule_names = [rule.value for rule in rule.incompatible_rules]
        conflicting_rule_names = sorted(
            list(set(rule_names).intersection(incompatible_rule_names))
        )
        if conflicting_rule_names:
            msg = (
                f"Validation rule: {rule.name.value} "
                f"has conflicting rules: {conflicting_rule_names}"
            )
            raise ValueError(msg)


def get_rule_from_inputted_rules(
    rule_name: ValidationRuleName, inputted_rules: list[str]
) -> Optional[str]:
    """Returns a rule from a list of rules

    Arguments:
        rule_name: A ValidationRuleName
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If there are multiple of the rule in the list

    Returns:
        The rule if one is found, otherwise None is returned
    """
    inputted_rules = [
        rule for rule in inputted_rules if rule.startswith(rule_name.value)
    ]
    if len(inputted_rules) > 1:
        raise ValueError(f"Found duplicates of rule in rules: {inputted_rules}")
    if len(inputted_rules) == 0:
        return None
    return inputted_rules[0]


def get_js_type_from_inputted_rules(
    inputted_rules: list[str],
) -> Optional[JSONSchemaType]:
    """Gets the JSON Schema type from a list of rules

    Arguments:
        inputted_rules: A list of inputted validation rules

    Raises:
        ValueError: If there are multiple type rules in the list

    Returns:
        The JSON Schema type if a type rule is found, otherwise None
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    validation_rules = _get_rules_by_names(rule_names)
    # A set of js_types of the validation rules
    json_schema_types = {
        rule.js_type for rule in validation_rules if rule.js_type is not None
    }
    if len(json_schema_types) > 1:
        raise ValueError(
            f"Validation rules contain more than one implied JSON Schema type: {inputted_rules}"
        )
    if len(json_schema_types) == 0:
        return None
    return list(json_schema_types)[0]


def get_in_range_parameters_from_inputted_rule(
    inputted_rule: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Returns the min and max from an inRange rule if they exist

    Arguments:
        inputted_rule: The inRange rule

    Returns:
        The min and max from the rule
    """
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    parameters = _get_parameters_from_inputted_rule(inputted_rule)
    if parameters:
        if (
            "minimum" in parameters
            and parameters["minimum"] is not None
            and parameters["minimum"].isnumeric()
        ):
            minimum = float(parameters["minimum"])
        if (
            "maximum" in parameters
            and parameters["maximum"] is not None
            and parameters["maximum"].isnumeric()
        ):
            maximum = float(parameters["maximum"])
    return (minimum, maximum)


def get_regex_parameters_from_inputted_rule(
    inputted_rule: str,
) -> Optional[str]:
    """
    Gets the pattern from the regex rule

    Arguments:
        inputted_rule: The full regex rule

    Returns:
        If the module parameter is search or match, and the pattern parameter exists
          the pattern is returned
        Otherwise None
    """
    module: Optional[str] = None
    pattern: Optional[str] = None
    parameters = _get_parameters_from_inputted_rule(inputted_rule)
    if parameters:
        if "module" in parameters:
            module = parameters["module"]
        if "pattern" in parameters:
            pattern = parameters["pattern"]
    if module is None or pattern is None:
        return None
    # Do not translate other modules
    if module not in [item.value for item in RegexModule]:
        return None
    # Match is just search but only at the beginning of the string
    if module == RegexModule.MATCH.value and not pattern.startswith("^"):
        return f"^{pattern}"
    return pattern


def get_validation_rule_names_from_inputted_rules(
    inputted_rules: list[str],
) -> list[ValidationRuleName]:
    """Gets a list of ValidationRuleNames from a list of inputted validation rules

    Arguments:
        inputted_rules: A list of inputted validation rules from a data model

    Returns:
        A list of ValidationRuleNames
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    rules = _get_rules_by_names(rule_names)
    return [rule.name for rule in rules]


def get_names_from_inputted_rules(inputted_rules: list[str]) -> list[str]:
    """Gets the names from a list of inputted rules

    Arguments:
        inputted_rules: A list of inputted validation rules from a data model

    Returns:
        The names of the inputted rules
    """
    return [_get_name_from_inputted_rule(rule) for rule in inputted_rules]


def _get_parameters_from_inputted_rule(inputted_rule: str) -> Optional[dict[str, str]]:
    """Creates a dictionary of parameters and values from an input rule string

    Arguments:
        inputted_rule: An inputted validation rule from a data model

    Returns:
        If the rule exists, a dictionary where
          the keys are the rule parameters
          the values are the input rule parameter values
        Else None
    """
    rule_name = _get_name_from_inputted_rule(inputted_rule)
    rule_values = inputted_rule.split(" ")[1:]
    rule = _VALIDATION_RULES.get(rule_name)
    if rule and rule.parameters:
        return dict(zip(rule.parameters, rule_values))
    return None


def _get_name_from_inputted_rule(inputted_rule: str) -> str:
    """Gets the name from an inputted rule

    Arguments:
        inputted_rule: An inputted validation rule from a data model

    Returns:
        The name of the inputted rule
    """
    return inputted_rule.split(" ")[0]


def _get_rules_by_names(names: list[str]) -> list[ValidationRule]:
    """Gets a list of ValidationRules by name if they exist

    Arguments:
        names: A list of names of ValidationRules

    Raises:
        ValueError: If any of the input names don't correspond to actual rules

    Returns:
        A list of ValidationRules
    """
    rule_dict = {name: _VALIDATION_RULES.get(name) for name in names}
    invalid_rule_names = [
        rule_name for (rule_name, rule) in rule_dict.items() if rule is None
    ]
    if invalid_rule_names:
        raise ValueError("Some input rule names are invalid:", invalid_rule_names)
    return [rule for rule in rule_dict.values() if rule is not None]
