"""This module contains functions for interacting with validation rules for JSON Schema creation"""

from dataclasses import dataclass
from typing import Optional
from schematic.schemas.constants import JSONSchemaType, ValidationRuleName, RegexModule


@dataclass
class ValidationRule:
    """
    This class represents a validation rule to be used for creating JSON Schemas

    Attributes:
        name: The name of the validation rule
        js_type: The JSON Schema type this rule indicates
        is_type_rule: Whether or not this rule directly corresponds to a JSON Schema type
        incompatible_rules: Other validation rules this rule can not be paired with
        parameters: Parameters for the validation rule that need to be collected for the JSON Schema
    """

    name: ValidationRuleName
    js_type: Optional[JSONSchemaType]
    is_type_rule: bool
    incompatible_rules: list[ValidationRuleName]
    parameters: Optional[list[str]] = None


# A List of validation rules currently collected to make a JSON Schema
_VALIDATION_RULES = [
    ValidationRule(
        name=ValidationRuleName.LIST,
        js_type=None,
        is_type_rule=False,
        incompatible_rules=[],
    ),
    ValidationRule(
        name=ValidationRuleName.DATE,
        js_type=JSONSchemaType.STRING,
        is_type_rule=False,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
        ],
    ),
    ValidationRule(
        name=ValidationRuleName.URL,
        js_type=JSONSchemaType.STRING,
        is_type_rule=False,
        incompatible_rules=[
            ValidationRuleName.DATE,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
        ],
    ),
    ValidationRule(
        name=ValidationRuleName.REGEX,
        js_type=JSONSchemaType.STRING,
        is_type_rule=False,
        incompatible_rules=[
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.BOOL,
            ValidationRuleName.NUM,
        ],
        parameters=["module", "pattern"],
    ),
    ValidationRule(
        name=ValidationRuleName.IN_RANGE,
        js_type=JSONSchemaType.NUMBER,
        is_type_rule=False,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
        ],
        parameters=["minimum", "maximum"],
    ),
    ValidationRule(
        name=ValidationRuleName.STR,
        js_type=JSONSchemaType.STRING,
        is_type_rule=True,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.NUM,
            ValidationRuleName.BOOL,
        ],
    ),
    ValidationRule(
        name=ValidationRuleName.FLOAT,
        js_type=JSONSchemaType.NUMBER,
        is_type_rule=True,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
        ],
    ),
    ValidationRule(
        name=ValidationRuleName.INT,
        js_type=JSONSchemaType.INTEGER,
        is_type_rule=True,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
        ],
    ),
    ValidationRule(
        name=ValidationRuleName.BOOL,
        js_type=JSONSchemaType.BOOLEAN,
        is_type_rule=True,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.INT,
            ValidationRuleName.FLOAT,
            ValidationRuleName.NUM,
        ],
    ),
    ValidationRule(
        name=ValidationRuleName.NUM,
        js_type=JSONSchemaType.NUMBER,
        is_type_rule=True,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
            ValidationRuleName.STR,
            ValidationRuleName.BOOL,
        ],
    ),
]


def filter_unused_inputted_rules(inputted_rules: list[str]) -> list[str]:
    """Filters a list of validation rules for only those used to create JSON Schemas

    Arguments:
        inputted_rules: A list of validation rules

    Returns:
        A filtered list of validation rules
    """
    return [
        rule
        for rule in inputted_rules
        if _get_name_from_inputted_rule(rule) in [e.value for e in ValidationRuleName]
    ]


def check_for_duplicate_inputted_rules(inputted_rules: list[str]) -> None:
    """Checks that there are no duplicate rules by name

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If there are multiple rules with the same name
    """
    rule_names = _get_names_from_inputted_rules(inputted_rules)
    if sorted(rule_names) != sorted(list(set(rule_names))):
        raise ValueError("Validation Rules contains duplicates: ", inputted_rules)


def check_for_conflicting_inputted_rules(inputted_rules: list[str]) -> None:
    """Checks that each rule has no conflicts with any other rule

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If a rule is in conflict with any other rule
    """
    rule_names = _get_names_from_inputted_rules(inputted_rules)
    rules: list[ValidationRule] = _get_rules_by_name(rule_names)
    for rule in rules:
        incompatible_rule_names = [rule.value for rule in rule.incompatible_rules]
        conflicting_rule_names = list(
            set(rule_names).intersection(incompatible_rule_names)
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
    """Returns the a rule from a list of rules

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
        raise ValueError("Found duplicates of rule in rules: ", inputted_rules)
    if len(inputted_rules) == 0:
        return None
    return inputted_rules[0]


def get_js_type_from_inputted_rules(inputted_rules: list[str]) -> Optional[str]:
    """Gets the JSON Schema type from a list of rules

    Arguments:
        inputted_rules: A list of inputted validation rules

    Raises:
        ValueError: If there are multiple type rules in the list

    Returns:
        The JSON Schema type if a type rule is found, otherwise None
    """
    rule_names = _get_names_from_inputted_rules(inputted_rules)
    validation_rules = _get_rules_by_name(rule_names)
    type_validation_rules = [rule for rule in validation_rules if rule.is_type_rule]
    if len(type_validation_rules) > 1:
        raise ValueError("Found more than one type rule: ", inputted_rules)
    if len(type_validation_rules) == 0:
        return None
    js_type = type_validation_rules[0].js_type
    assert js_type is not None
    return js_type.value


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


def _get_parameters_from_inputted_rule(inputted_rule: str) -> Optional[dict[str, str]]:
    """Creates a dictionary of parameters and values from and input rule string

    Args:
        input_rule: An inputted validation rule form a data model

    Returns:
        If the rule exists, a dictionary where
          the keys are the rule parameters
          the values are the input rule parameter values
        Else None
    """
    rule_name = _get_name_from_inputted_rule(inputted_rule)
    rule_values = inputted_rule.split(" ")[1:]
    rule = _get_rule_by_name(rule_name)
    if rule and rule.parameters:
        return dict(zip(rule.parameters, rule_values))
    return None


def _get_names_from_inputted_rules(inputted_rules: list[str]) -> list[str]:
    """Gets the names from a list of inputted rules

    Arguments:
        inputted_rules: A list of inputted validation rules form a data model

    Returns:
        The names of the inputted rules
    """
    return [_get_name_from_inputted_rule(rule) for rule in inputted_rules]


def _get_name_from_inputted_rule(inputted_rule: str) -> str:
    """Gets the name from an inputted rule

    Arguments:
        inputted_rule: An inputted validation rule form a data model

    Returns:
        The name of the inputted rule
    """
    return inputted_rule.split(" ")[0]


def _get_rules_by_name(names: list[str]) -> list[ValidationRule]:
    """Gets a list of ValidationRules by name of they exist

    Args:
        names: A list of names of ValidationRules

    Returns:
        A list of ValidationRules
    """
    rules = [_get_rule_by_name(name) for name in names]
    return [rule for rule in rules if rule is not None]


def _get_rule_by_name(name: str) -> Optional[ValidationRule]:
    """Gets a ValidationRule by its name if it exists

    Args:
        name: The name of the ValidationRule

    Returns:
        The ValidationRule if it exists, otherwise None
    """
    rules = [rule for rule in _VALIDATION_RULES if rule.name.value == name]
    if len(rules) == 0:
        return None
    return rules[0]
