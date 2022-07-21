import os
from jsonschema import validate
from re import compile
from schematic.utils.io_utils import load_json
from schematic import CONFIG, LOADER


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

