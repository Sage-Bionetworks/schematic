# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

import os

import pandas as pd

from schema_explorer import SchemaExplorer


"""
Utility for converting csv file following a data model definition schema (see 
scRNA-seq.csv for an example) into schema.org schema
"""
#TODO: provide a generic template csv 

def get_class(display_class_name: str, description: str = None, subclass_of: list = ["Thing"], requires_dependencies: list = None, requires_range: list  = None) -> dict:
    
    """Constructs a new schema.org compliant class given a set of schema object attributes

    Args:
       display_class_name: human readable label for the schema object/attribute: key characteristic X of the assay, related protocol, or downstream data that we want to record as metadata feature
       description: definition or a reference containing the definition of attribute X. Preferably provide a source ontology link or code in addition to the definition.
       subclass_of: *schema* label of this attribute/object's parent node in the schema
       requires_dependencies: important characteristics, if any, of attribute X that need to be recorded as metadata features given attribute X is specified. These characteristics are attributes themselves and need to pre-exist in the schema as such
       requires_range: a set/range of values that this attribute can be assigned to. this domain is stored in the rangeIncludes property of this object. 

    Returns: a json schema.org object
    """

    class_name = se.get_class_label_from_display_name(display_name)

    class_attributes = {
                    '@id': 'bts:'+class_name,
                    '@type': 'rdfs:Class',
                    'rdfs:comment': description if description else "TBD",
                    'rdfs:label': class_name,
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'}
    }

    if subclass_of:
        parent = {'rdfs:subClassOf':[{'@id':'bts:' + sub} for sub in subclass_of]}
        class_attributes.update(parent)

    if requires_dependencies:
        requirement = {'sms:requiresDependency':[{'@id':'bts:' + dep} for dep in requires_dependencies]}
        class_attributes.update(requirement)

    if requires_range:
        value_constraint = {'schema:rangeIncludes':[{'@id':'bts:' + val} for val in requires_range]}
        class_attributes.update(value_constraint)
    
    if display_name:
        class_attributes.update({'sms:displayName':display_name})

    return class_attributes


def get_property(property_display_name: str, property_class_name: str, description: str = None, allowed_values: str = 'Text') -> dict:

    """Constructs a new schema.org compliant property of an existing schema.org object/class; note that the property itself is a schema.org opject class.

    Args:
        property_display_name: human readable label for the schema object/attribute: key characteristic X of the assay, related protocol, or downstream data that we want to record as metadata feature
        property_class_name: *schema* label of the class/object that this is a property of
       description: definition or a reference containing the definition of attribute X. Preferably provide a source ontology link or code in addition to the definition.
       allowed_values: what is the set/domain of values that this attribute can be assigned to; currently only used to specify primitive types. TODO: extend to reg exp patterns 

    Returns: a json schema.org  property object
    """

    new_property = {
                    '@id': 'bts:' + property_name,
                    '@type': 'rdf:Property',
                    'rdfs:comment': description if description else "",
                    'rdfs:label': property_name,
                    'schema:domainIncludes': {'@id': 'bts:' + property_class_name},
                    'schema:rangeIncludes': {'@id': 'bts:' + allowed_values},
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'},
    }
                    
    #'http://schema.org/domainIncludes':{'@id': 'bts:' + property_class_name},
    #'http://schema.org/rangeIncludes':{'@id': 'schema:' + allowed_values},

    return new_property



# required headers for schema; may or may not abstract further; for now hardcode
required_headers = set(["Attribute", "Description", "Valid Values", "Requires", "Required", "Parent, Properties"])


def check_schema_definition(schema_definition: pd.DataFrame) -> bool:

    """Checks if a schema definition data frame contains the right required headers.

    See schema definition guide for more details
    TODO: post and link schema definition guide
       
    Args: 
        schema_definition: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0 

    Returns: True if schema definition headers are valid; False otherwise
    """

    if set(schema_definition.columns.columns) == required_headers:
        return True

    return False


def create_schema_classes(schema_extension: pd.DataFrame, se: SchemaExplorer, base_schema_path: str) -> SchemaExplorer:
    
    """Creates classes for all attributes and adds them to the schema
    Args:
        schema_extension: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0 
        se: a schema explorer object allowing the traversal and modification of a schema graph        
        base_schema_path: a path to a json-ld file containing an existing schema

    Returns: an updated schema explorer object
    """

    # instantiate schema explorer
    se = SchemaExplorer()

    # load existing challenge base schema
    se.load_schema(base_schema_path)
        
    # get attributes from Attribute column
    attributes = schema_extension[["Attribute", "Description", "Parent", "Valid Values"]].to_dict("records")

    for attribute in attributes:
        display_name = attribute["Attribute"]
        new_class = get_class(display_name,\
                                      description = attribute["Description"],\
                                      subclass_of = [parent for parent in attribute["Parent"].strip().split(",")]
        )
        se.update_class(new_class)


    # get attribute properties from Properties column
    properties= schema_extension[["Attribute", "Properties"]].to_dict("records")

    for prop in properties:
        if prop["Properties"]: # a class may have or not properties
            for p in prop["Properties"].strip().split(","): # a class may have multiple properties
                attribute = prop["Attribute"]
                if p in schema_extension["Attribute"]:
                    description = schema_extension.loc[schema_extension["Properties"][p]]["Description"]
                else: 
                    description = None

                new_property = get_property(p,\
                                            attribute,
                                            description = description
                )

                se.update_property(new_property)
                

    # set required values and dependency requirements for each attribute
    

        


