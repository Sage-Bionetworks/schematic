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


def parse_str_series_to_list(col: pd.Series) -> pd.Series:
    """
    Parse a pandas series of comma delimited strings
    into a series with values that are lists of strings
    ex.
        Input:  'a,b,c'
        Output: ['a','b','c']

    """
    col = col.apply(
        lambda x: [s.strip() for s in str(x).split(",")] if not pd.isnull(x) else pd.NA
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
        rule: str, attribute: str, rule_modifiers: list[str], validation_expectation: dict[str,str],
    ) -> bool:
        """Need to determine if required is the only rule being set. Do this way so we dont have
        to enforce a position for it (ie, it can only be before message and after the rule).
        This ensures that 'required' is not treated like a real rule, in the case it is
        accidentally combined with a rule modifier. The required rule is t

        Args:
            rule: str, the validation rule string
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
            only_rule = True
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
                    f"with modifiers attached.",
                    f"Rule modifiers do not work in conjunction with the required validation rule.",
                    f"Please reformat your rule.",
                ]
            )
            logger.warning(warning_message)
            return True

