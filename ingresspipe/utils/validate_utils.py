import os
from .io_utils import load_json
from jsonschema import validate

_ROOT = "./data/"

def validate_schema(schema):
    """Validate schema against schema.org standard
    """
    json_schema_path = os.path.join(_ROOT, 'validation_schemas', 'schema.json')
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_property_schema(schema):
    """Validate schema against SchemaORG property definition standard
    """
    json_schema_path = os.path.join(_ROOT, 'validation_schemas', 'property_json_schema.json')
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_class_schema(schema):
    """Validate schema against SchemaORG class definition standard
    """
    json_schema_path = os.path.join(_ROOT, 'validation_schemas', 'class_json_schema.json')
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)