"""A module for constants and enums to be shared across the schemas module"""

from enum import Enum

class ValidationRule(Enum):
    """Validation rules that are used to create JSON Schema"""

    REGEX = "regex"
    IN_RANGE = "inRange"
    STR = "str"
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    NUM = "num"


class JSONSchemaType(Enum):
    """This enum is allowed values type values for a JSON Schema in a data model"""

    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"


class RegexModule(Enum):
    """This enum are allowed modules for the regex validation rule"""

    SEARCH = "search"
    MATCH = "match"


# A dict where the keys are type validation rules, and the values are their JSON Schema equivalent
TYPE_RULES = {
    ValidationRule.STR.value: JSONSchemaType.STRING.value,
    ValidationRule.NUM.value: JSONSchemaType.NUMBER.value,
    ValidationRule.FLOAT.value: JSONSchemaType.NUMBER.value,
    ValidationRule.INT.value: JSONSchemaType.INTEGER.value,
    ValidationRule.BOOL.value: JSONSchemaType.BOOLEAN.value,
}
