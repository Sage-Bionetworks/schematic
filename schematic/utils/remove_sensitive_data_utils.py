"""remove sensitive data from a string utils"""
from typing import Dict
import re


def redact_string(value: str) -> str:
    """remove sensitive data from a string

    Args:
        value (str): a string that may contain sensitive data

    Returns:
        str: remove sensitive data from string
    """
    sensitive_patterns = {
        "google_sheets": r"https://sheets\.googleapis\.com/v\d+/spreadsheets/[\w-]+"
    }
    _compiled_patterns = {
        name: re.compile(pattern) for name, pattern in sensitive_patterns.items()
    }
    redacted = value
    for pattern_name, pattern in _compiled_patterns.items():
        redacted = pattern.sub(f"[REDACTED_{pattern_name.upper()}]", redacted)
    return redacted


def redacted_sensitive_data_in_exception(
    exception_attributes: Dict[str, str]
) -> Dict[str, str]:
    """remove sensitive data in exception

    Args:
        exception_attributes (dict):a dictionary of exception attributes

    Returns:
        dict: a dictionary of exception attributes with sensitive data redacted
    """
    redacted_exception_attributes = {}
    for key, value in exception_attributes.items():
        # remove sensitive information from exception message and stacktrace
        if key in ("exception.message", "exception.stacktrace"):
            redacted_exception_attributes[key] = redact_string(value)
        else:
            redacted_exception_attributes[key] = value
    return redacted_exception_attributes
