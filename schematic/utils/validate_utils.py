"""Validation utils"""

# pylint: disable = anomalous-backslash-in-string

import re
from collections.abc import Mapping
import logging
from typing import Pattern, Union, Iterable, Any, Optional
from numbers import Number
from jsonschema import validate
import numpy as np
import pandas as pd
from schematic.utils.io_utils import load_json
from schematic import LOADER

logger = logging.getLogger(__name__)


def validate_schema(schema: Union[Mapping, bool]) -> None:
    """Validate schema against schema.org standard"""
    data_path = "validation_schemas/model.schema.json"
    json_schema_path = LOADER.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_property_schema(schema: Union[Mapping, bool]) -> None:
    """Validate schema against SchemaORG property definition standard"""
    data_path = "validation_schemas/property.schema.json"
    json_schema_path = LOADER.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_class_schema(schema: Union[Mapping, bool]) -> None:
    """Validate schema against SchemaORG class definition standard"""
    data_path = "validation_schemas/class.schema.json"
    json_schema_path = LOADER.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def comma_separated_list_regex() -> Pattern[str]:
    """
    Regex to match with comma separated list
    Requires at least one element and a comma to be valid
    Does not require a trailing comma

    Returns:
        Pattern[str]:
    """
    csv_list_regex = re.compile("([^\,]+\,)(([^\,]+\,?)*)")

    return csv_list_regex


def convert_nan_entries_to_empty_strings(
    manifest: pd.core.frame.DataFrame,
) -> pd.core.frame.DataFrame:
    """
    Nans need to be converted to empty strings for JSON Schema Validation. This helper
    converts an a list with a single '<NA>' string or a single np.nan to empty strings.
    These types of expected NANs come from different stages of conversion during import
    and validation.

    Args:
        manifest: pd.core.frame.DataFrame, manifest prior to removing nans and
            replacing with empty strings.
    Returns:
        manifest: pd.core.frame.DataFrame, manifest post removing nans and
            replacing with empty strings.
    """
    # Replace nans with empty strings so jsonschema, address replace type infering depreciation.
    with pd.option_context("future.no_silent_downcasting", True):
        manifest = manifest.replace({np.nan: ""}).infer_objects(copy=False)  # type: ignore

    for col in manifest.columns:
        for index, value in manifest[col].items():
            if value == ["<NA>"]:
                manifest.loc[index, col] = [""]  # type: ignore
    return manifest


def rule_in_rule_list(rule: str, rule_list: list[str]) -> Optional[re.Match[str]]:
    """
    Function to standardize
    checking to see if a rule is contained in a list of rules.
    Uses regex to avoid issues arising from validation rules with arguments
    or rules that have arguments updated.
    """
    # separate rule type if arguments are specified
    rule_type = rule.split(" ")[0]

    # Process string and list of strings for regex comparison
    rule_type = rule_type + "[^\|]*"
    rule_list_str = "|".join(rule_list)
    return re.search(rule_type, rule_list_str, flags=re.IGNORECASE)


def get_list_robustness(val_rule: str) -> str:
    """Helper function to extract list robustness from the validation rule.
    List robustness defines if the input -must- be a list (several values
    or a single value with a trailing comma),
    or if a user is allowed to submit a single value.
    List rules default to `strict` if not defined to be `like`
    Args:
        val_rule: str, validation rule string.
    Returns:
        list_robutness: str, list robustness extracted from validation rule.
    """
    list_robustness_options = ["like", "strict"]
    list_robustness = None
    default_robustness = list_robustness_options[1]

    # Get the parts of a single rule, list is assumed to be in the first position, based on
    # requirements that can be found in documentation.
    rule_parts = val_rule.lower().split(" ")

    if len(rule_parts) > 1:
        # Check if list_robustness is defined in the rule, if not give them the default.
        list_robustness_list = [
            part for part in rule_parts if part in list_robustness_options
        ]
        if list_robustness_list:
            list_robustness = list_robustness_list[0]

    if not list_robustness:
        # If no robustness has been defined by the user, set to the default.
        list_robustness = default_robustness
    return list_robustness


def parse_str_series_to_list(col: pd.Series, replace_null: bool = True) -> pd.Series:
    """
    Parse a pandas series of comma delimited strings
    into a series with values that are lists of strings. If replace_null, fill null values
    with nan. If the type of the value needs to be an array, fill with empty list.
    ex.
        Input:  'a,b,c'
        Output: ['a','b','c']

    """
    if replace_null:
        col = col.apply(
            lambda x: [s.strip() for s in str(x).split(",")]
            if not pd.isnull(x)
            else pd.NA
        )
    else:
        col = col.apply(
            lambda x: [s.strip() for s in str(x).split(",")]
            if (isinstance(x, np.ndarray) and not x.any()) or not pd.isnull(x)
            else []
        )

    return col


def np_array_to_str_list(np_array: Any) -> list[str]:
    """
    Parse a numpy array of ints to a list of strings
    """
    return np.char.mod("%d", np_array).tolist()


def iterable_to_str_list(obj: Union[str, Number, Iterable]) -> list[str]:
    """
    Parse an object into a list of strings
    Accepts str, Number, and iterable inputs
    """

    # If object is a string, just return wrapped as a list
    if isinstance(obj, str):
        return [obj]
    # If object is numerical, convert to string and wrap as a list
    if isinstance(obj, Number):
        return [str(obj)]
    # If the object is iterable and not a string, convert every element
    # to string and wrap as a list
    return [str(item) for item in obj]


def required_is_only_rule(
    rule: str,
    attribute: str,
    rule_modifiers: list[str],
    validation_expectation: dict[str, str],
) -> bool:
    """Need to determine if required is the only rule being set. Do this way so we dont have
    to enforce a position for it (ie, it can only be before message and after the rule).
    This ensures that 'required' is not treated like a real rule, in the case it is
    accidentally combined with a rule modifier. The required rule is t

    Args:
        rule: str, the validation rule string
        attribute: str, attribute the validation rule is set to
        rule_modifiers: list[str], list of rule modifiers available to add to rules
        validation_expectation: dict[str, str], currently implemented expectations.
    Returns:
        bool, True, if required is the only rule, false if it is not.
    """
    # convert rule to lowercase to ensure punctuation does not throw off determination.
    rule = rule.lower()

    # If required is not in the rule, it cant be the only rule, return False
    if "required" not in rule:
        return False

    # If the entire rule is just 'required' then it is easily determined to be the only rule
    if rule == "required":
        return True

    # Try to find an expectation rule in the rule, if there is one there log it and
    # continue
    # This function is called as part of an if that is already looking for in house rules
    # so don't worry about looking for them.
    rule_parts = rule.split(" ")
    for idx, rule_part in enumerate(rule_parts):
        if rule_part in validation_expectation:
            return False

    # identify then remove all rule modifiers, all that should be left is required in the
    # case that someone used a standard modifier with required
    idx_to_remove = []
    if "required" in rule_parts:
        for idx, rule_part in enumerate(rule_parts):
            if rule_part in rule_modifiers:
                idx_to_remove.append(idx)

    if idx_to_remove:
        for idx in sorted(idx_to_remove, reverse=True):
            del rule_parts[idx]

    # In this case, rule modifiers have been added to required. This is not the expected use
    # so log a warning, but let user proceed.
    if rule_parts == ["required"]:
        warning_message = " ".join(
            [
                f"For Attribute: {attribute}, it looks like required was set as a single rule,"
                "with modifiers attached.",
                "Rule modifiers do not work in conjunction with the required validation rule.",
                "Please reformat your rule.",
            ]
        )
        logger.warning(warning_message)
        return True

    # Return false if no other condition has been met. In this case if the rule is not a real
    # rule an error will be raised from the containing function.
    return False
