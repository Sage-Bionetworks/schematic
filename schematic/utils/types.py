"""Custom types used throughout Schematic"""

from typing import TypeAlias

# Recursive type used for JSON-like dictionaries
JsonType: TypeAlias = dict[str, "JsonType"] | list["JsonType"] | str | int | float | bool | None
