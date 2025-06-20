"""Unit tests for validation rule functions"""

from typing import Optional, Union
import pytest
from schematic.schemas.json_schema_validation_rule_functions import (
    ValidationRuleName,
    filter_unused_inputted_rules,
    check_for_duplicate_inputted_rules,
    check_for_conflicting_inputted_rules,
    get_rule_from_inputted_rules,
    get_js_type_from_inputted_rules,
    get_in_range_parameters_from_inputted_rule,
    get_regex_parameters_from_inputted_rule,
    get_validation_rule_names_from_inputted_rules,
    _get_parameters_from_inputted_rule,
    get_names_from_inputted_rules,
    _get_name_from_inputted_rule,
    _get_rules_by_name,
)
from schematic.schemas.constants import JSONSchemaType


@pytest.mark.parametrize(
    "input_list, expected_list",
    [
        ([], []),
        (["str"], ["str"]),
        (["str", "unused_rule"], ["str"]),
    ],
    ids=["No rules", "Str rule", "Contains an unused rule"],
)
def test_filter_unused_inputted_rules(
    input_list: list[str], expected_list: list[str]
) -> None:
    """
    Test for filter_unused_inputted_rules
    Tests that only rules used to create JSON Schemas are left
    """
    result = filter_unused_inputted_rules(input_list)
    assert result == expected_list


@pytest.mark.parametrize(
    "input_list",
    [
        ([]),
        (["str"]),
        (["str", "int", "bool"]),
    ],
    ids=["No rules", "One rule", "Multiple rules"],
)
def test_check_for_duplicate_inputted_rules(
    input_list: list[str],
) -> None:
    """
    Test for check_for_duplicate_inputted_rules
    Tests that no duplicate rules were found and no exceptions raised
    """
    check_for_duplicate_inputted_rules(input_list)


@pytest.mark.parametrize(
    "input_list",
    [(["str", "str"]), (["str warning", "str error"])],
    ids=["Duplicate string rules", "Duplicate string rules with different parameters"],
)
def test_check_for_duplicate_inputted_rules_with_duplicates(
    input_list: list[str],
) -> None:
    """
    Test for check_for_duplicate_inputted_rules
    Tests that duplicate rules were found and a ValueError was raised
    """
    with pytest.raises(ValueError, match="Validation Rules contains duplicates"):
        check_for_duplicate_inputted_rules(input_list)


@pytest.mark.parametrize(
    "input_list",
    [([]), (["str", "regex"]), (["inRange", "int"])],
    ids=["No rules", "Str and regex rules", "InRange and int rules"],
)
def test_check_for_conflicting_inputted_rules(
    input_list: list[str],
) -> None:
    """
    Test for check_for_conflicting_inputted_rules
    Tests that no rules are in conflict with each other
    """
    check_for_conflicting_inputted_rules(input_list)


@pytest.mark.parametrize(
    "input_list, expected_msg",
    [
        (["str", "int"], "Validation rule: str has conflicting rules: \\['int'\\]"),
        (["date", "url"], "Validation rule: date has conflicting rules: \\['url'\\]"),
        (["regex", "int"], "Validation rule: regex has conflicting rules: \\['int'\\]"),
        (
            ["inRange", "str"],
            "Validation rule: inRange has conflicting rules: \\['str'\\]",
        ),
        (
            ["inRange", "str", "regex"],
            "Validation rule: inRange has conflicting rules: \\['regex', 'str'\\]",
        ),
    ],
    ids=[
        "Multiple type rules",
        "Multiple format rules",
        "Regex and int rules",
        "InRange and str rules",
        "InRange and multiple conflicting rules",
    ],
)
def test_check_for_conflicting_inputted_rules_with_conflicts(
    input_list: list[str], expected_msg: str
) -> None:
    """
    Test for check_for_conflicting_inputted_rules
    Tests that rules are in conflict with each other and a ValueError is raised
    """
    with pytest.raises(ValueError, match=expected_msg):
        check_for_conflicting_inputted_rules(input_list)


@pytest.mark.parametrize(
    "rule, input_rules, expected_rule",
    [
        (ValidationRuleName.IN_RANGE, [], None),
        (ValidationRuleName.IN_RANGE, ["regex match [a-f]"], None),
        (ValidationRuleName.IN_RANGE, ["inRange 0 1"], "inRange 0 1"),
        (ValidationRuleName.IN_RANGE, ["str error", "inRange 0 1"], "inRange 0 1"),
        (ValidationRuleName.REGEX, ["inRange 0 1"], None),
        (ValidationRuleName.REGEX, ["regex match [a-f]"], "regex match [a-f]"),
    ],
    ids=[
        "inRange: No rules",
        "inRange: No inRange rules",
        "inRange: Rule present",
        "inRange: Rule present, multiple rules",
        "regex: No regex rules",
        "regex: Rule present",
    ],
)
def test_get_rule_from_inputted_rules(
    rule: ValidationRuleName,
    input_rules: list[str],
    expected_rule: Optional[str],
) -> None:
    """
    Test for get_rule_from_inputted_rules
    Tests that None is returned if there are no matches
      or the rule is returned if there is a match
    """
    result = get_rule_from_inputted_rules(rule, input_rules)
    assert result == expected_rule


def test_get_rule_from_inputted_rules_with_exception() -> None:
    """
    Test for get_rule_from_inputted_rules
    Tests that when the requested rule has multiple matches, a ValueError is raised
    """
    with pytest.raises(ValueError):
        get_rule_from_inputted_rules(
            ValidationRuleName.IN_RANGE, ["inRange", "inRange 0 1"]
        )


@pytest.mark.parametrize(
    "input_rules, expected_js_type",
    [
        ([], None),
        (["list strict"], None),
        (["str"], JSONSchemaType.STRING),
        (["str error"], JSONSchemaType.STRING),
    ],
    ids=["No rules", "List", "String", "String with error param"],
)
def test_get_js_type_from_inputted_rules(
    input_rules: list[str],
    expected_js_type: Optional[JSONSchemaType],
) -> None:
    """
    Test for get_js_type_from_inputted_rules
    Tests that if theres only one JSON Schema type amongst all the rules it will be returned
      Otherwise None will be returned
    """
    result = get_js_type_from_inputted_rules(input_rules)
    assert result == expected_js_type


def test_get_js_type_from_inputted_rules_with_exception() -> None:
    """
    Test for get_js_type_from_inputted_rules
    Tests that if there are multiple JSON Schema types amongst all the rules
      a ValueError will be raised
    """
    with pytest.raises(ValueError):
        get_js_type_from_inputted_rules(["str", "int"])


@pytest.mark.parametrize(
    "input_rule, expected_tuple",
    [
        ("inRange", (None, None)),
        ("inRange x x", (None, None)),
        ("inRange 0", (0, None)),
        ("inRange 0 x", (0, None)),
        ("inRange 0 1", (0, 1)),
        ("inRange 0 1 x", (0, 1)),
    ],
    ids=[
        "inRange with no params",
        "inRange with bad params",
        "inRange with minimum",
        "inRange with minimum, bad maximum",
        "inRange with minimum, maximum",
        "inRange with minimum, maximum, extra param",
    ],
)
def test_get_in_range_parameters_from_inputted_rule(
    input_rule: str,
    expected_tuple: tuple[Optional[str], Optional[str]],
) -> None:
    """
    Test for get_in_range_parameters_from_inputted_rule
    Tests that if the minimum and maximum parameters exist and are numeric they are returned
    """
    result = get_in_range_parameters_from_inputted_rule(input_rule)
    assert result == expected_tuple


@pytest.mark.parametrize(
    "input_rule, expected_pattern",
    [
        ("regex search [a-f]", "[a-f]"),
        ("regex match [a-f]", "^[a-f]"),
        ("regex match ^[a-f]", "^[a-f]"),
        ("regex split ^[a-f]", None),
    ],
    ids=[
        "Search module, Pattern returned",
        "Match module, Pattern returned with carrot added",
        "Match module, Pattern returned with no carrot added",
        "Unallowed module, None returned",
    ],
)
def test_get_regex_parameters_from_inputted_rule(
    input_rule: str,
    expected_pattern: Optional[str],
) -> None:
    """
    Test for get_regex_parameters_from_inputted_rule
    Tests that if the module parameter exists and is one of the allowed values
      the pattern is returned
    """
    result = get_regex_parameters_from_inputted_rule(input_rule)
    assert result == expected_pattern


@pytest.mark.parametrize(
    "input_rules, expected_rule_names",
    [
        ([], []),
        (["not_a_rule"], []),
        (["str"], [ValidationRuleName.STR]),
        (["str error"], [ValidationRuleName.STR]),
        (
            ["str", "regex", "not_a_rule"],
            [ValidationRuleName.STR, ValidationRuleName.REGEX],
        ),
    ],
    ids=[
        "Empty list",
        "No actual rules",
        "String rule",
        "String rule with parameters",
        "Multiple rules",
    ],
)
def test_get_validation_rule_names_from_inputted_rules(
    input_rules: list[str], expected_rule_names: list[ValidationRuleName]
) -> None:
    """
    Test for get_validation_rule_names_from_inputted_rules
    Tests that for each input rule, the ValidationRuleName is returned if it exists
    """
    result = get_validation_rule_names_from_inputted_rules(input_rules)
    assert result == expected_rule_names


@pytest.mark.parametrize(
    "input_rule, expected_dict",
    [
        ("not a rule", None),
        ("str", None),
        ("str error", None),
        ("regex", {}),
        ("regex search", {"module": "search"}),
        ("regex search [a-f]", {"module": "search", "pattern": "[a-f]"}),
    ],
    ids=[
        "Not a rule",
        "Str rule no parameters",
        "Str rule parameters, but not collected",
        "Regex rule no parameters",
        "Regex rule module parameter",
        "Regex rule module parameter and pattern parameter",
    ],
)
def test_get_parameters_from_inputted_rule(
    input_rule: str,
    expected_dict: Optional[dict[str, Union[str, float]]],
) -> None:
    """
    Test for _get_parameters_from_inputted_rule
    Tests that if the validation rule has parameters to collect, and the input rule has them
      they will be collected as a dict.
    """
    result = _get_parameters_from_inputted_rule(input_rule)
    assert result == expected_dict


@pytest.mark.parametrize(
    "rules, expected_rule_names",
    [
        ([], []),
        (["str"], ["str"]),
        (["str warning"], ["str"]),
        (["str warning", "regex search [a-f]"], ["str", "regex"]),
    ],
    ids=[
        "Empty",
        "One string rule, no parameters",
        "One string rule, with parameters",
        "two rules with parameters",
    ],
)
def test_get_names_from_inputted_rules(
    rules: list[str], expected_rule_names: list[str]
) -> None:
    """
    Test for get_names_from_inputted_rules
    Tests that the rule name is returned for each rule
    (A rule is a string, that when split by spaces, the first item is the name)
    """
    result = get_names_from_inputted_rules(rules)
    assert result == expected_rule_names


@pytest.mark.parametrize(
    "rules, expected_rule_names",
    [("str", "str"), ("str warning", "str"), ("regex search [a-f]", "regex")],
    ids=[
        "String rule, no parameters",
        "String rule, with parameters",
        "Regex rule, with parameters",
    ],
)
def test_get_name_from_inputted_rule(
    rules: list[str], expected_rule_names: list[str]
) -> None:
    """
    Test for _get_name_from_inputted_rule
    Tests that the rule name is returned
    (A rule is a string, that when split by spaces, the first item is the name)
    """
    result = _get_name_from_inputted_rule(rules)
    assert result == expected_rule_names


@pytest.mark.parametrize(
    "rule_names, expected_rule_names",
    [
        (["not_a_rule"], []),
        (["str"], [ValidationRuleName.STR]),
        (
            ["str", "regex", "not_a_rule"],
            [ValidationRuleName.STR, ValidationRuleName.REGEX],
        ),
    ],
    ids=["Not a rule", "Str rule", "Str, regex, and a non-rule"],
)
def test_get_rules_by_name(
    rule_names: list[str], expected_rule_names: list[ValidationRuleName]
) -> None:
    """
    Test for _get_rules_by_name
    Tests that for every actual rule name in the input list the rule is returned
    """
    result = _get_rules_by_name(rule_names)
    result_rules_names = [rule.name for rule in result]
    assert result_rules_names == expected_rule_names
