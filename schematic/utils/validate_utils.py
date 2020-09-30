import os
from schematic.utils.io_utils import load_json
from jsonschema import validate

from definitions import DATA_PATH

def validate_schema(schema):
    """Validate schema against schema.org standard
    """
    json_schema_path = os.path.join(DATA_PATH, 'validation_schemas', 'schema.json')
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_property_schema(schema):
    """Validate schema against SchemaORG property definition standard
    """
    json_schema_path = os.path.join(DATA_PATH, 'validation_schemas', 'property_json_schema.json')
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_class_schema(schema):
    """Validate schema against SchemaORG class definition standard
    """
    json_schema_path = os.path.join(DATA_PATH, 'validation_schemas', 'class_json_schema.json')
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)