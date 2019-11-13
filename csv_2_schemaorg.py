# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

import os

import pandas as pd
import numpy as np

from schema_explorer import SchemaExplorer


"""
Utility for converting csv file following a data model definition schema (see 
scRNA-seq.csv for an example) into schema.org schema
"""
#TODO: provide a generic template csv 

def get_class(se: SchemaExplorer, class_display_name: str, description: str = None, subclass_of: list = ["Thing"], requires_dependencies: list = None, requires_range: list  = None) -> dict:
    
    """Constructs a new schema.org compliant class given a set of schema object attributes

    Args:
       se: a schema explorer object allowing the traversal and modification of a schema graph        
       display_class_name: human readable label for the schema object/attribute: key characteristic X of the assay, related protocol, or downstream data that we want to record as metadata feature
       description: definition or a reference containing the definition of attribute X. Preferably provide a source ontology link or code in addition to the definition.
       subclass_of: *schema* label of this attribute/object's parent node in the schema
       requires_dependencies: important characteristics, if any, of attribute X that need to be recorded as metadata features given attribute X is specified. These characteristics are attributes themselves and need to pre-exist in the schema as such
       requires_range: a set/range of values that this attribute can be assigned to. this domain is stored in the rangeIncludes property of this object. 

    Returns: a json schema.org object
    """

    class_name = se.get_class_label_from_display_name(class_display_name)

    class_attributes = {
                    '@id': 'bts:'+class_name,
                    '@type': 'rdfs:Class',
                    'rdfs:comment': description if description else "TBD",
                    'rdfs:label': class_name,
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'}
    }

    if subclass_of:
        parent = {'rdfs:subClassOf':[{'@id':'bts:' + se.get_class_label_from_display_name(sub)} for sub in subclass_of]}
        class_attributes.update(parent)

    if requires_dependencies:
        requirement = {'sms:requiresDependency':[{'@id':'bts:' + se.get_class_label_from_display_name(dep)} for dep in requires_dependencies]}
        class_attributes.update(requirement)

    if requires_range:
        value_constraint = {'schema:rangeIncludes':[{'@id':'bts:' + se.get_class_label_from_display_name(val)} for val in requires_range]}
        class_attributes.update(value_constraint)
    
    class_attributes.update({'sms:displayName':class_display_name})
    
    return class_attributes


def get_property(se: SchemaExplorer, property_display_name: str, property_class_name: str, description: str = None, requires_range: list = None, requires_dependencies: list = None) -> dict:

    """Constructs a new schema.org compliant property of an existing schema.org object/class; note that the property itself is a schema.org object class.

    Args:
        se: a schema explorer object allowing the traversal and modification of a schema graph        
        property_display_name: human readable label for the schema object/attribute: key characteristic X of the assay, related protocol, or downstream data that we want to record as metadata feature
        property_class_name: *schema* label of the class/object that this is a property of
       description: definition or a reference containing the definition of attribute X. Preferably provide a source ontology link or code in addition to the definition.
       requires_range: what is the set/domain of values that this attribute can be assigned to; currently only used to specify primitive types. TODO: extend to reg exp patterns 
       requires_dependencies: important characteristics, if any, of property X that need to be recorded as metadata features given property X is specified. These characteristics are attributes themselves and need to pre-exist in the schema as such


    Returns: a json schema.org  property object
    """
    property_name = se.get_property_label_from_display_name(property_display_name)

    property_attributes = {
                    '@id': 'bts:' + property_name,
                    '@type': 'rdf:Property',
                    'rdfs:comment': description if description else "",
                    'rdfs:label': property_name,
                    'sms:displayName': property_display_name,
                    'schema:domainIncludes': {'@id': 'bts:' + se.get_class_label_from_display_name(property_class_name)},
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'},
    }
    if requires_range:
        value_constraint = {'schema:rangeIncludes':[{'@id':'bts:' + se.get_class_label_from_display_name(val)} for val in requires_range]}
        property_attributes.update(value_constraint)
    
    if requires_dependencies:
        requirement = {'sms:requiresDependency':[{'@id':'bts:' + se.get_class_label_from_display_name(dep)} for dep in requires_dependencies]}
        property_attributes.update(requirement)
    
    #'http://schema.org/domainIncludes':{'@id': 'bts:' + property_class_name},
    #'http://schema.org/rangeIncludes':{'@id': 'schema:' + allowed_values},
    
    property_attributes.update({'sms:displayName':property_display_name})

    return property_attributes



# required headers for schema; may or may not abstract further; for now hardcode
required_headers = set(["Attribute", "Description", "Valid Values", "Requires", "Required", "Parent", "Properties"])


def check_schema_definition(schema_definition: pd.DataFrame) -> bool:

    """Checks if a schema definition data frame contains the right required headers.

    See schema definition guide for more details
    TODO: post and link schema definition guide
       
    Args: 
        schema_definition: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0
    Raises: Exception
    """
    
    if set(list(schema_definition.columns)) == required_headers:
        return
    raise Exception()


def create_schema_classes(schema_extension: pd.DataFrame, se: SchemaExplorer) -> SchemaExplorer:
    
    """Creates classes for all attributes and adds them to the schema
    Args:
        schema_extension: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0 
        se: a schema explorer object allowing the traversal and modification of a schema graph        
        base_schema_path: a path to a json-ld file containing an existing schema

    Returns: an updated schema explorer object
    """

    try:
        check_schema_definition(schema_extension)
        print("Schema definition csv ready for processing!")
    except:
        print("Schema extension headers: ")     
        print(set(list(schema_extension.columns)))
        print("do not match required schema headers: ")
        print(required_headers)
        print("ERROR: could not add extension " + schema_extension_csv + " to schema!")
        exit()

    # get attributes from Attribute column
    attributes = schema_extension[["Attribute", "Description", "Parent", "Valid Values", "Requires"]].to_dict("records")
    
    # get all properties across all attributes from Property column
    all_properties = set(schema_extension[["Properties"]].values.flatten())
    
    # get attribute properties from Properties column
    properties= schema_extension[["Attribute", "Properties"]].to_dict("records")

    #TODO: check if schema already contains attribute - may require attribute context in csv schema definition
    for attribute in attributes:
        if not attribute["Attribute"] in all_properties:
            display_name = attribute["Attribute"]
            new_class = get_class(se, display_name,
                                          description = attribute["Description"],
                                          subclass_of = [parent for parent in attribute["Parent"].strip().split(",")]
            )
            se.update_class(new_class)

        else:
            display_name = attribute["Attribute"]
            new_property = get_property(se, display_name,
                                          attribute["Attribute"],
                                          description = attribute["Description"],
            )
            se.update_property(new_property)


    #TODO check if schema already contains property - may require property context in csv schema definition
    for prop in properties:
        if not pd.isnull(prop["Properties"]): # a class may have or not have properties
            for p in prop["Properties"].strip().split(","): # a class may have multiple properties
                attribute = prop["Attribute"]

                # check if property is already present as attribute under attributes column
                # TODO: adjust logic below to compactify code
                if p in schema_extension["Attribute"]:
                    description = schema_extension.loc[schema_extension["Attribute"][p]]["Description"] 
                    property_info = se.explore_property(se.get_class_label_from_display_name(p))
                    range_values = property_info["range"] if "range" in property_info else None
                    requires_dependencies = property_info["dependencies"] if "dependencies" in property_info else None
                        
                    new_property = get_property(se, p,
                                                property_info["domain"],
                                                description = description,
                                                requires_range = range_values,
                                                requires_dependencies = requires_dependencies
                    )
                    se.edit_property(new_property)
                else: 
                    description = None
                    new_property = get_property(se, p,
                                                attribute,
                                                description = description
                    ) 
                    se.update_property(new_property)


    # set range values and dependency requirements for each attribute
    # if not already added, add each attribute in required values and dependencies to the schema extension 
    for attribute in attributes:

        # TODO: refactor processing of multi-valued cells in columns and corresponding schema updates; it would compactify code below if class and property are encapsulated as objects inheriting from a common attribute parent object

        # get values in range for this attribute, if any are specified
        range_values = attribute["Valid Values"]
        if not pd.isnull(range_values):
            for val in range_values.strip().split(","):
                # check if value is in attributes column; add it as a class if not
                #TODO: maintain a list of added classes and properties and only add if not already added
                if not val in schema_extension["Attribute"]:
                    new_class = get_class(se, val,
                                          description = None,
                                          subclass_of = [attribute["Attribute"]]
                    )
                    se.update_class(new_class)
                                    
                #update rangeIncludes of attribute
                # if attribute is not a property, then assume it is a class
                if not attribute["Attribute"] in all_properties:
                    class_info = se.explore_class(se.get_class_label_from_display_name(attribute["Attribute"]))
                    class_range_edit = get_class(se, attribute["Attribute"],
                                                  description = class_info["description"],\
                                                  subclass_of = class_info["subClassOf"],\
                                                  requires_dependencies = class_info["dependencies"],\
                                                  requires_range = class_info["range"].append(se.get_class_label_from_display_name(val))
                    )
                    se.edit_class(class_range_edit)
                else:
                # the attribute is a property
                    property_info = se.explore_property(se.get_property_label_from_display_name(attribute["Attribute"]))
                    property_info["range"].append(se.get_class_label_from_display_name(val))
                    property_range_edit = get_property(se, attribute["Attribute"],
                                                       property_info["domain"],
                                                       description = property_info["description"],
                                                       requires_dependencies = property_info["dependencies"],
                                                       requires_range = property_info["range"]
                    )
                    se.edit_property(property_range_edit)
            

        # get dependencies for this attribute, if any are specified
        requires_dependencies = attribute["Requires"]
        if not pd.isnull(requires_dependencies):
            for dep in requires_dependencies.strip().split(","):
                # check if dependency is a property or not
                dep_is_property = dep in all_properties

                dep_label = ""
                # set dependency label based on kind of dependency: class or property 
                if dep_is_property:
                    dep_label = se.get_property_label_from_display_name(dep) 
                else:
                    dep_label = se.get_class_label_from_display_name(dep)

              
                # check if dependency is in attributes column; add it to the list if not
                if not dep in schema_extension["Attribute"]:
                    # if dependency is a property create a new property; else create a new class
                    if not dep_is_property:
                        new_class = get_class(se, dep,
                                              description = None, 
                                              subclass_of = [attribute["Attribute"]]
                        )
                        se.update_class(new_class)
                    else:
                        description = None
                        new_property = get_property(se, dep,
                                                    attribute["Attribute"],
                                                    description = description
                        ) 
                        se.update_property(new_property)

                # update required dependencies of attribute
                # if attribute is not a property then assume it is a class
                if not attribute["Attribute"] in all_properties:
                    class_info = se.explore_class(se.get_class_label_from_display_name(attribute["Attribute"]))
                    class_info["dependencies"].append(dep_label)
                    class_dependencies_edit = get_class(se, attribute["Attribute"],
                                                  description = class_info["description"],
                                                  subclass_of = class_info["subClassOf"],
                                                  requires_dependencies = class_info["dependencies"], 
                                                  requires_range = class_info["range"]
                                                  
                    )
                    se.edit_class(class_dependencies_edit)
                else:
                    # the attribute is a property then update as a property
                    property_info = se.explore_property(se.get_property_label_from_display_name(attribute["Attribute"]))
                    property_info["dependencies"].append(dep_label)
                    property_dependencies_edit = get_property(se, attribute["Attribute"],
                                                       property_info["domain"],
                                                       description = property_info["description"],
                                                       requires_dependencies = property_info["dependencies"],
                                                       requires_range = property_info["range"]
                    )
                    se.edit_property(property_dependencies_edit)

    return se
