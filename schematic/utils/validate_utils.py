import os
from schematic.utils.io_utils import load_json
from jsonschema import validate

from schematic import CONFIG
from schematic.loader import Loader, InvalidResourceError

# call Loader() and pass `schematic`, which is the global package namespace
loader = Loader('schematic', prefix='etc')

def validate_schema(schema):
    """Validate schema against schema.org standard
    """
    data_path = 'validation/schema.json'
    json_schema_path = loader.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_property_schema(schema):
    """Validate schema against SchemaORG property definition standard
    """
    data_path = 'validation/property_json_schema.json'
    json_schema_path = loader.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)


def validate_class_schema(schema):
    """Validate schema against SchemaORG class definition standard
    """
    data_path = 'validation/class_json_schema.json'
    json_schema_path = loader.filename(data_path)
    json_schema = load_json(json_schema_path)
    return validate(schema, json_schema)
