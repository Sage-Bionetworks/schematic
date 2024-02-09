import inflection
import json
import networkx as nx
import re
import string
from typing import List, Dict, Tuple, Union

DELIMITERS = {
    "component_name_delimiter": "#",
    "component_rules_delimiter": "^^",
    "rule_delimiter": "::",
}


def attr_dict_template(key_name: str) -> Dict[str, dict[str, dict]]:
    return {key_name: {"Relationships": {}}}


def get_property_label_from_display_name(
    display_name: str, strict_camel_case: bool = False
) -> str:
    """Convert a given display name string into a proper property label string
    Args:
        display_name, str: node display name
        strict_camel_case, bool: Default, False; defines whether or not to use strict camel case or not for conversion.
    Returns:
        label, str: property label of display name
    """
    # This is the newer more strict method
    if strict_camel_case:
        display_name = display_name.strip().translate(
            {ord(c): "_" for c in string.whitespace}
        )
        label = inflection.camelize(display_name, uppercase_first_letter=False)

    # This method remains for backwards compatibility
    else:
        display_name = display_name.translate({ord(c): None for c in string.whitespace})
        label = inflection.camelize(display_name.strip(), uppercase_first_letter=False)

    return label


def get_class_label_from_display_name(
    display_name: str, strict_camel_case: bool = False
) -> str:
    """Convert a given display name string into a proper class label string
    Args:
        display_name, str: node display name
        strict_camel_case, bool: Default, False; defines whether or not to use strict camel case or not for conversion.
    Returns:
        label, str: class label of display name
    """
    # This is the newer more strict method
    if strict_camel_case:
        display_name = display_name.strip().translate(
            {ord(c): "_" for c in string.whitespace}
        )
        label = inflection.camelize(display_name, uppercase_first_letter=True)

    # This method remains for backwards compatibility
    else:
        display_name = display_name.translate({ord(c): None for c in string.whitespace})
        label = inflection.camelize(display_name.strip(), uppercase_first_letter=True)

    return label


def get_attribute_display_name_from_label(
    node_name: str, attr_relationships: dict
) -> str:
    """Get attribute display name for a node, using the node label, requires the attr_relationships dicitonary from the data model parser
    Args:
        node_name, str: node label
        attr_relationships, dict: dictionary defining attributes and relationships, generated in data model parser.
    Returns:
        display_name, str: node display name, recorded in attr_relationships.
    """
    if "Attribute" in attr_relationships.keys():
        display_name = attr_relationships["Attribute"]
    else:
        display_name = node_name
    return display_name


def get_label_from_display_name(
    display_name: str, entry_type: str, strict_camel_case: bool = False
) -> str:
    """Get node label from provided display name, based on whether the node is a class or property
    Args:
        display_name, str: node display name
        entry_type, str: 'class' or 'property', defines what type the entry is.
        strict_camel_case, bool: Default, False; defines whether or not to use strict camel case or not for conversion.
    Returns:
        label, str: class label of display name
    Raises:
        ValueError if entry_type.lower(), is not either 'class' or 'property'

    """
    if entry_type.lower() == "class":
        label = get_class_label_from_display_name(
            display_name=display_name, strict_camel_case=strict_camel_case
        )

    elif entry_type.lower() == "property":
        label = get_property_label_from_display_name(
            display_name=display_name, strict_camel_case=strict_camel_case
        )
    else:
        raise ValueError(
            f"The entry type submitted: {entry_type}, is not one of the permitted types: 'class' or 'property'"
        )
    return label


def convert_bool_to_str(provided_bool: bool) -> str:
    """Convert bool to string.
    Args:
        provided_bool, str: true or false bool
    Returns:
        Boolean converted to 'true' or 'false' str as appropriate.
    """
    return str(provided_bool)


def get_component_rules(component_rule: str, validation_rules: list) -> list:
    # Separate multiple rules (defined by addition of the rule delimiter)
    if DELIMITERS["rule_delimiter"] in component_rule:
        validation_rules.append(component_rule.split(DELIMITERS["rule_delimiter"]))
    # Get single rule
    else:
        validation_rules.append(component_rule)
    return validation_rules


def get_component_name(component_names: list, component_rule: str) -> Tuple[list, str]:
    # If a component name is not attached to the rule, have it apply to all other components
    if DELIMITERS["component_name_delimiter"] != component_rule[0]:
        component_names.append("all_other_components")
    # Get the component name if available
    else:
        component_names.append(
            component_rule.split(" ")[0].replace(
                DELIMITERS["component_name_delimiter"], ""
            )
        )
        try:
            assert component_names[-1] != " "
        except:
            ValueError(
                f"There was an error capturing at least one of the component name in the following rule: {component_rule}, "
                f"please ensure there is not extra whitespace or non-allowed characters."
            )
        component_rule = component_rule.replace(component_rule.split(" ")[0], "")
        component_rule = component_rule.strip()
    return component_names, component_rule


def parse_component_validation_rules(validation_rule_string: str) -> Dict:
    component_names = []
    validation_rules = []

    component_rules = validation_rule_string.split(
        DELIMITERS["component_rules_delimiter"]
    )
    # Extract component rules, per component
    for component_rule in component_rules:
        component_rule = component_rule.strip()
        if component_rule:
            # Get component name attached to rule
            component_names, component_rule = get_component_name(
                component_names=component_names, component_rule=component_rule
            )

            # Get rules
            validation_rules = get_component_rules(
                component_rule=component_rule, validation_rules=validation_rules
            )

    # Ensure we collected the component names and validation rules like expected
    try:
        assert len(component_names) == len(validation_rules)
    except:
        raise ValueError(
            f"The number of components names and validation rules does not match for validation rule: {validation_rule_string}."
        )

    validation_rules_dict = dict(
        map(lambda i, j: (i, j), component_names, validation_rules)
    )
    return validation_rules_dict


def parse_single_set_validation_rules(validation_rule_string: str) -> list:
    # Try to catch an improperly formatted rule
    if DELIMITERS["component_name_delimiter"] == validation_rule_string[0]:
        raise ValueError(
            f"The provided validation rule {validation_rule_string}, looks to be formatted as a component "
            f"based rule, but is missing the necessary formatting, "
            f"please refer to the SchemaHub documentation for more details."
        )

    # Parse rules that are set across *all* components/manifests
    if DELIMITERS["rule_delimiter"] in validation_rule_string:
        return validation_rule_string.split(DELIMITERS["rule_delimiter"])


def parse_validation_rules(validation_rules: Union[list, dict]) -> Union[list, dict]:
    """Split multiple validation rules based on :: delimiter
    Args:
        validation_rules, Any[List[str], Dict]: List or Dictionary of validation rules,
            if list, contains a string validation rule; if dictionary, key is the component the
            rule (value) is applied to
    Returns:
        validation_rules, list: if submitted List
    Raises:
        ValueError if Rule is not formatted properly
    """

    if isinstance(validation_rules, dict):
        # Rules pulled in as a dict can be used directly
        return validation_rules
    elif isinstance(validation_rules, list):
        validation_rule_string = validation_rules[0]
        # Parse rules set for a subset of components/manifests
        if DELIMITERS["component_rules_delimiter"] in validation_rule_string:
            return parse_component_validation_rules(
                validation_rule_string=validation_rule_string
            )
        # Parse rules that are set across *all* components/manifests
        else:
            return parse_single_set_validation_rules(
                validation_rule_string=validation_rule_string
            )
    else:
        raise ValueError(
            f"The validation rule provided: {str(validation_rules)} is not submitted in an accepted type (list, dictionary) please check your JSONLD."
        )


def extract_component_validation_rules(
    manifest_component: str, validation_rules: dict
) -> list:
    manifest_component_rule = validation_rules.get(manifest_component)
    all_component_rules = validation_rules.get("all_other_components")
    if manifest_component_rule:
        if type(manifest_component_rule) == str:
            validation_rules = [manifest_component_rule]
        else:
            validation_rules = manifest_component_rule
    elif all_component_rules:
        if type(all_component_rules) == str:
            validation_rules = [all_component_rules]
        else:
            validation_rules = all_component_rules
    else:
        validation_rules = []
    return validation_rules


def export_schema(schema: dict, file_path: str) -> None:
    """Export schema to given filepath.
    Args:
        schema, dict: JSONLD schema
        filepath, str: path to store the schema
    """
    with open(file_path, "w") as f:
        json.dump(schema, f, sort_keys=True, indent=4, ensure_ascii=False)


def strip_context(context_value: str) -> tuple[str]:
    """Strip contexts from str entry.
    Args:
        context_value, str: string from which to strip context from
    Returns:
        context, str: the original context
        v, str: value separated from context
    """
    if ":" in context_value:
        context, v = context_value.split(":")
    elif "@" in context_value:
        context, v = context_value.split("@")
    return context, v
