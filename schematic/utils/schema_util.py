'''
General methods.

'''

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