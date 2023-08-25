import inflection
import json
import networkx as nx
import string


def get_property_label_from_display_name(display_name, strict_camel_case = False):
        """Convert a given display name string into a proper property label string"""
        """
        label = ''.join(x.capitalize() or ' ' for x in display_name.split(' '))
        label = label[:1].lower() + label[1:] if label else ''
        """
        # This is the newer more strict method
        if strict_camel_case:
            display_name = display_name.strip().translate({ord(c): "_" for c in string.whitespace})
            label = inflection.camelize(display_name, uppercase_first_letter=False)

        # This method remains for backwards compatibility
        else:
            display_name = display_name.translate({ord(c): None for c in string.whitespace})
            label = inflection.camelize(display_name.strip(), uppercase_first_letter=False)

        return label

def get_class_label_from_display_name(display_name, strict_camel_case = False):
    """Convert a given display name string into a proper class label string"""
    """
    label = ''.join(x.capitalize() or ' ' for x in display_name.split(' '))"""
    # This is the newer more strict method
    if strict_camel_case:
        display_name = display_name.strip().translate({ord(c): "_" for c in string.whitespace})
        label = inflection.camelize(display_name, uppercase_first_letter=True)

    # This method remains for backwards compatibility
    else:
        display_name = display_name.translate({ord(c): None for c in string.whitespace})
        label = inflection.camelize(display_name.strip(), uppercase_first_letter=True)

    return label

def get_display_name_from_label(node_name, attr_relationships):
    '''
    TODO: if not display name raise error.
    '''
    if 'Attribute' in attr_relationships.keys():
        display_name = attr_relationships['Attribute']
    else:
        display_name = node_name
    return display_name

def get_label_from_display_name(display_name, entry_type, strict_camel_case = False):
    
    if entry_type.lower()=='class':
        label = get_class_label_from_display_name(display_name=display_name, strict_camel_case=strict_camel_case)
    
    elif entry_type.lower()=='property':
        label=get_property_label_from_display_name(display_name=display_name, strict_camel_case=strict_camel_case)
    return label

def convert_bool(provided_bool):
    return str(provided_bool)

def parse_validation_rules(validation_rules:list) -> list:
    if validation_rules and '::' in validation_rules[0]:
        validation_rules = validation_rules[0].split('::')
    return validation_rules

def export_schema(schema, file_path):
    with open(file_path, "w") as f:
        json.dump(schema, f, sort_keys=True, indent=4, ensure_ascii=False)
