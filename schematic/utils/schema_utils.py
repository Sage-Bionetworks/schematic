"""Schema utils"""

# pylint: disable=logging-fstring-interpolation

import json
import logging
import os
import string
from typing import Literal, Union, Optional

import inflection


logger = logging.getLogger(__name__)

DisplayLabelType = Literal["class_label", "display_label"]
EntryType = Literal["class", "property"]
BLACKLISTED_CHARS = ["(", ")", ".", " ", "-"]
COMPONENT_NAME_DELIMITER = "#"
COMPONENT_RULES_DELIMITER = "^^"
RULE_DELIMITER = "::"


def attr_dict_template(key_name: str) -> dict[str, dict[str, dict]]:
    """Create a single empty attribute_dict template.

    Args:
        key_name (str): Attribute/node to use as the key in the dict.

    Returns:
        dict[str, dict[str, dict]]: template single empty attribute_relationships dictionary
    """
    return {key_name: {"Relationships": {}}}


def get_property_label_from_display_name(
    display_name: str, strict_camel_case: bool = False
) -> str:
    """Convert a given display name string into a proper property label string
    Args:
        display_name, str: node display name
        strict_camel_case, bool: Default, False; defines whether or not to use
          strict camel case or not for conversion.
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
        strict_camel_case, bool: Default, False; defines whether or not to
         use strict camel case or not for conversion.
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
    """
    Get attribute display name for a node, using the node label, requires the attr_relationships
      dictionary from the data model parser

    Args:
        node_name, str: node label
        attr_relationships, dict: dictionary defining attributes and relationships,
          generated in data model parser.
    Returns:
        display_name, str: node display name, recorded in attr_relationships.
    """
    if "Attribute" in attr_relationships.keys():
        display_name = attr_relationships["Attribute"]
    else:
        display_name = node_name
    return display_name


def check_if_display_name_is_valid_label(
    display_name: str,
    blacklisted_chars: Optional[list[str]] = None,
) -> bool:
    """Check if the display name can be used as a display label

    Args:
        display_name (str): node display name
        blacklisted_chars (Optional[list[str]], optional):
          characters that are not permitted for synapse annotations uploads.
          Defaults to None.

    Returns:
        bool: True, if the display name can be used as a label, False, if it cannot.
    """
    if blacklisted_chars is None:
        blacklisted_chars = BLACKLISTED_CHARS
    valid_label = not any(char in display_name for char in blacklisted_chars)
    return valid_label


def get_stripped_label(
    display_name: str,
    entry_type: EntryType,
    blacklisted_chars: Optional[list[str]] = None,
) -> str:
    """
    Args:
        display_name, str: node display name
        entry_type, EntryType: 'class' or 'property', defines what type the entry is.
        blacklisted_chars, list[str]: characters that are not permitted for
          synapse annotations uploads.
    Returns:
        stripped_label, str: class or property label that has been stripped
          of blacklisted characters.
    """
    if blacklisted_chars is None:
        blacklisted_chars = BLACKLISTED_CHARS
    if entry_type.lower() == "class":
        stripped_label = [
            get_class_label_from_display_name(str(display_name)).translate(
                {ord(x): "" for x in blacklisted_chars}
            )
        ][0]

    elif entry_type.lower() == "property":
        stripped_label = [
            get_property_label_from_display_name(str(display_name)).translate(
                {ord(x): "" for x in blacklisted_chars}
            )
        ][0]

    logger.warning(
        (
            f"Cannot use display name {display_name} as the data model label, "
            "because it is not formatted properly. Please remove all spaces and "
            f"blacklisted characters: {str(blacklisted_chars)}. "
            f"The following label was assigned instead: {stripped_label}"
        )
    )
    return stripped_label


def get_schema_label(
    display_name: str, entry_type: EntryType, strict_camel_case: bool
) -> str:
    """Get the class or property label for a given display name
    Args:
        display_name, str: node display name
        entry_type, EntryType: 'class' or 'property', defines what type the entry is.
        strict_camel_case, bool: Default, False; defines whether or not to use strict
          camel case or not for conversion.
    Returns:
        label, str: class label of display name
    Raises:
        Error Logged if entry_type.lower(), is not either 'class' or 'property'
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
        logger.error(
            (
                f"The entry type submitted: {entry_type}, is not one of the "
                "permitted types: 'class' or 'property'"
            )
        )
    return label


def get_label_from_display_name(
    display_name: str,
    entry_type: EntryType,
    strict_camel_case: bool = False,
    data_model_labels: DisplayLabelType = "class_label",
) -> str:
    """Get node label from provided display name, based on whether the node is a class or property
    Args:
        display_name, str: node display name
        entry_type, EntryType: 'class' or 'property', defines what type the entry is.
        strict_camel_case, bool: Default, False; defines whether or not to use strict camel
          case or not for conversion.
    Returns:
        label, str: label to be used for the provided display name.
    """
    if data_model_labels == "display_label":
        # Check that display name can be used as a label.
        valid_display_name = check_if_display_name_is_valid_label(
            display_name=display_name
        )
        # If the display name is valid, set the label to be the display name
        if valid_display_name:
            label = display_name
        # If not, set get a stripped class or property label (as indicated by the entry type)
        else:
            label = get_stripped_label(display_name=display_name, entry_type=entry_type)

    else:
        label = get_schema_label(
            display_name=display_name,
            entry_type=entry_type,
            strict_camel_case=strict_camel_case,
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


def get_individual_rules(rule: str, validation_rules: list) -> list:
    """Extract individual rules from a string and add to a list of rules
    Args:
        rule, str: validation rule that has been parsed from a component rule.
        validation_rules, list: list of rules being collected,
            if this is the first time the list is being added to, it will be empty
    Returns:
        validation_rules, list: list of rules being collected.
    """
    # Separate multiple rules (defined by addition of the rule delimiter)
    if RULE_DELIMITER in rule:
        validation_rules.append(parse_single_set_validation_rules(rule))
    # Get single rule
    else:
        validation_rules.append(rule)
    return validation_rules


def get_component_name_rules(
    component_names: list[str], component_rule: str
) -> tuple[list[str], str]:
    """
    Get component name and rule from an string that was initially split by the
      COMPONENT_RULES_DELIMITER

    Args:
        component_names, list[str]: list of components, will be empty
          if being added to for the first time.
        component_rule, str: component rule string that has only been split by
          the COMPONENT_RULES_DELIMITER
    Returns:
        Tuple[list,str]: list with the a new component name or 'all_other_components' appended,
            rule with the component name stripped off.
    Raises:
        Error Logged if it looks like a component name should have been added to the
          list, but was not.
    """
    # If a component name is not attached to the rule, have it apply to all other components
    if COMPONENT_NAME_DELIMITER != component_rule[0]:
        component_names.append("all_other_components")
    # Get the component name if available
    else:
        component_names.append(
            component_rule.split(" ")[0].replace(COMPONENT_NAME_DELIMITER, "")
        )
        if component_names[-1] == " ":
            logger.error(
                f"There was an error capturing at least one of the component names "
                f"in the following rule: {component_rule}, "
                f"please ensure there is not extra whitespace or non-allowed characters."
            )

        component_rule = component_rule.replace(component_rule.split(" ")[0], "")
        component_rule = component_rule.strip()
    return component_names, component_rule


def check_for_duplicate_components(
    component_names: list[str], validation_rule_string: str
) -> None:
    """
    Check if component names are repeated in a validation rule
    Error Logged if a component name is duplicated.

    Args:
        component_names (list[str]): list of components identified in the validation rule
        validation_rule_string (str): validation rule, used if error needs to be raised.
    """
    duplicated_entries = [cn for cn in component_names if component_names.count(cn) > 1]
    if duplicated_entries:
        logger.error(
            f"Oops, it looks like the following rule {validation_rule_string}, "
            "contains the same component name more than once. An attribute can "
            "only have a single rule applied per manifest/component."
        )


def parse_component_validation_rules(
    validation_rule_string: str,
) -> dict[str, list[str]]:
    """
    If a validation rule is identified to be formatted as a component validation rule,
      parse to a dictionary of components:rules

    Args:
        validation_rule_string (str):  validation rule provided by user.

    Returns:
        dict[str, list[str]]: validation rules parsed to a dictionary where the key
          is the component name (or 'all_other_components') and the value is the parsed
          validation rule for the given component.
    """
    component_names: list[str] = []
    validation_rules: list[list[str]] = []

    component_rules = validation_rule_string.split(COMPONENT_RULES_DELIMITER)
    # Extract component rules, per component
    for component_rule in component_rules:
        component_rule = component_rule.strip()
        if component_rule:
            # Get component name attached to rule
            component_names, component_rule = get_component_name_rules(
                component_names=component_names, component_rule=component_rule
            )

            # Get rules
            validation_rules = get_individual_rules(
                rule=component_rule, validation_rules=validation_rules
            )

    # Ensure we collected the component names and validation rules like expected
    if len(component_names) != len(validation_rules):
        logger.error(
            f"The number of components names and validation rules does not match "
            f"for validation rule: {validation_rule_string}."
        )

    # If a component name is repeated throw an error.
    check_for_duplicate_components(component_names, validation_rule_string)

    validation_rules_dict = dict(zip(component_names, validation_rules))

    return validation_rules_dict


def parse_single_set_validation_rules(validation_rule_string: str) -> list[str]:
    """Parse a single set of validation rules.

    Args:
        validation_rule_string (str): validation rule provided by user.

    Returns:
        list: the validation rule string split by the rule delimiter
    """
    # Try to catch an improperly formatted rule
    if COMPONENT_NAME_DELIMITER == validation_rule_string[0]:
        logger.error(
            f"The provided validation rule {validation_rule_string}, looks to be formatted as a "
            "component based rule, but is missing the necessary formatting, "
            "please refer to the SchemaHub documentation for more details."
        )

    return validation_rule_string.split(RULE_DELIMITER)


def parse_validation_rules(validation_rules: Union[list, dict]) -> Union[list, dict]:
    """Split multiple validation rules based on :: delimiter

    Args:
        validation_rules (Union[list, dict]): List or Dictionary of validation rules,
            if list:, contains a string validation rule
            if dict:, key is the component the rule (value) is applied to

    Returns:
        Union[list, dict]: Parsed validation rules, component rules are output
          as a dictionary, single sets are a list.
    """
    if isinstance(validation_rules, dict):
        # Rules pulled in as a dict can be used directly
        return validation_rules

    # If rules are already parsed from the JSONLD
    if len(validation_rules) > 1 and isinstance(validation_rules[-1], str):
        return validation_rules
    # Parse rules set for a subset of components/manifests
    if COMPONENT_RULES_DELIMITER in validation_rules[0]:
        return parse_component_validation_rules(
            validation_rule_string=validation_rules[0]
        )
    # Parse rules that are set across *all* components/manifests
    return parse_single_set_validation_rules(validation_rule_string=validation_rules[0])


def extract_component_validation_rules(
    manifest_component: str, validation_rules_dict: dict[str, Union[list, str]]
) -> list[Union[str, list]]:
    """
    Parse a component validation rule dictionary to pull out the rule (if any) for a given manifest

    Args:
        manifest_component, str: Component label, pulled from the manifest directly
        validation_rules_dict, dict[str, list[Union[list,str]]: Validation rules dictionary,
          where keys are the manifest component label, and the value is a parsed set of
          validation rules.
    Returns:
        validation_rules, list[str]: rule for the provided manifest component if one is available,
            if a validation rule is not specified for a given component but "all_other_components"
            is specified (as a key), then pull that one, otherwise return an empty list.
    """
    manifest_component_rule = validation_rules_dict.get(manifest_component)
    all_component_rules = validation_rules_dict.get("all_other_components")

    # Capture situation where manifest_component rule is an empty string
    if manifest_component_rule is not None:
        if isinstance(manifest_component_rule, str):
            if manifest_component_rule == "":
                validation_rules_list: list[Union[str, list]] = []
            else:
                validation_rules_list = [manifest_component_rule]
        elif isinstance(manifest_component_rule, list):
            validation_rules_list = manifest_component_rule
    elif all_component_rules:
        if isinstance(all_component_rules, str):
            validation_rules_list = [all_component_rules]
        elif isinstance(all_component_rules, list):
            validation_rules_list = all_component_rules
    else:
        validation_rules_list = []
    return validation_rules_list


def export_schema(schema: dict, file_path: str) -> None:
    """Export schema to given filepath.
    Args:
        schema, dict: JSONLD schema
        filepath, str: path to store the schema
    """
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(schema, json_file, sort_keys=True, indent=4, ensure_ascii=False)


def strip_context(context_value: str) -> tuple[str, str]:
    """Strip contexts from str entry.
    Args:
        context_value, str: string from which to strip context from
    Returns:
        context, str: the original context
        value, str: value separated from context
    """
    if ":" in context_value:
        context, value = context_value.split(":")
    elif "@" in context_value:
        context, value = context_value.split("@")
    return context, value


def get_json_schema_log_file_path(data_model_path: str, source_node: str) -> str:
    """Get json schema log file name from the data_mdoel_path
    Args:
        data_model_path: str, path to the data model
        source_node: str, root node to create the JSON schema for
    Returns:
        json_schema_log_file_path: str, file name for the log file
    """
    data_model_path_root, _ = os.path.splitext(data_model_path)
    prefix = data_model_path_root
    prefix_root, prefix_ext = os.path.splitext(prefix)
    if prefix_ext == ".model":
        prefix = prefix_root
    json_schema_log_file_path = f"{prefix}.{source_node}.schema.json"
    return json_schema_log_file_path
