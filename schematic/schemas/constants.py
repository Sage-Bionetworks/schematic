"""A module for constants and enums to be shared across the schemas module"""

from enum import Enum


class ValidationRuleName(Enum):
    """Names of validation rules that are used to create JSON Schema"""

    LIST = "list"
    DATE = "date"
    URL = "url"
    REGEX = "regex"
    IN_RANGE = "inRange"
    STR = "str"
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    NUM = "num"


class JSONSchemaType(Enum):
    """This enum is the currently supported JSON Schema types"""

    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"


class JSONSchemaFormat(Enum):
    """This enum is the currently supported JSON Schema formats"""

    DATE = "date"
    URI = "uri"


class RegexModule(Enum):
    """This enum are allowed modules for the regex validation rule"""

    SEARCH = "search"
    MATCH = "match"
