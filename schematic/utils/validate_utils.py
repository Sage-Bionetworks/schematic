import os
import pandas as pd
from jsonschema import validate
from re import compile, search, IGNORECASE
from schematic.utils.io_utils import load_json
from schematic import CONFIG, LOADER
from typing import List
import numpy as np

def validate_schema(schema):
    """Validate schema against schema.org standard"""
    data_path = "validation_schemas/model.schema.json"
    json_schema_path = LOADER.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_property_schema(schema):
    """Validate schema against SchemaORG property definition standard"""
    data_path = "validation_schemas/property.schema.json"
    json_schema_path = LOADER.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_class_schema(schema):
    """Validate schema against SchemaORG class definition standard"""
    data_path = "validation_schemas/class.schema.json"
    json_schema_path = LOADER.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)

def comma_separated_list_regex():
    # Regex to match with comma separated list 
    # Requires at least one element and a comma to be valid 
    # Does not require a trailing comma
    csv_list_regex=compile('([^\,]+\,)(([^\,]+\,?)*)')

    return csv_list_regex

def rule_in_rule_list(rule: str, rule_list: List[str]):
    # Function to standardize 
    # checking to see if a rule is contained in a list of rules. 
    # Uses regex to avoid issues arising from validation rules with arguments 
    # or rules that have arguments updated.

    # seperate rule type if arguments are specified
    rule_type = rule.split(" ")[0]

    # Process string and list of strings for regex comparison
    rule_type = rule_type + '[^\|]*'
    rule_list = '|'.join(rule_list)

    return search(rule_type, rule_list, flags=IGNORECASE)

def parse_str_series_to_list(col: pd.Series):
    """
    Parse a pandas series of comma delimited strings
    into a series with values that are lists of strings 
    ex. 
        Input:  'a,b,c'
        Output: ['a','b','c']     

    """
    col = col.apply(
        lambda x: [s.strip() for s in str(x).split(",")]
    )

    return col

def np_array_to_str_list(np_array):
    """
    Parse a numpy array of ints to a list of strings
    """
    return np.char.mod('%d', np_array).tolist()

def iterable_to_str_list(iterable):
    "Parse an iterable into a list of strings"
    strlist = []

    for element in iterable:
        strlist.append(str(element))

    return strlist
    