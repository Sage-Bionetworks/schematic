# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

import os
import string

import pandas as pd
import numpy as np

import re

from schematic.schemas.explorer import SchemaExplorer

"""
Utility for converting csv file containing a data model definition schema (see scRNA-seq.csv for an example) into schema.org schema.
"""

# required headers for schema; may or may not abstract further; for now hardcode
required_headers = set(["Attribute", "Description", "Valid Values", "Requires", "Required", "Parent", "Properties", "Requires Component", "Source", "Validation Rules"])


def get_class(se: SchemaExplorer, class_display_name: str, description: str = None, subclass_of: list = None, requires_dependencies: list = None, requires_range: list  = None, requires_components: list = None, required:bool = None, validation_rules: list = None) -> dict:
    
    """Constructs a new schema.org compliant class given a set of schema object attributes

    Args:
       se: a schema explorer object allowing the traversal and modification of a schema graph        
       display_class_name: human readable label for the schema object/attribute: key characteristic X of the assay, related protocol, or downstream data that we want to record as metadata feature
       description: definition or a reference containing the definition of attribute X. Preferably provide a source ontology link or code in addition to the definition.
       subclass_of: *schema* label of this attribute/object's parent node in the schema
       requires_dependencies: important characteristics, if any, of attribute X that need to be recorded as metadata features given attribute X is specified. These characteristics are attributes themselves and need to pre-exist in the schema as such
       requires_range: a set/range of values that this attribute can be assigned to. this domain is stored in the rangeIncludes property of this object. 
       requires_components: a set of associated components/categories that this object/entity requires for its full specification; each component is a high level ontology class in which entities/objects are categorized/componentized and it is an entity on its own that needs to exist in the schema.
       required: indicates if this attribute is required or optional in a schema
       validation_rules: a list of validation rules defined for this class (e.g. defining what is a valid object of this class)

    Returns: a json schema.org object
    """

    class_name = se.get_class_label_from_display_name(class_display_name)

    # setup biothings object template with mandatory elements
    class_attributes = {
                    '@id': 'bts:'+class_name,
                    '@type': 'rdfs:Class',
                    'rdfs:comment': description if description and not pd.isnull(description) else "TBD",
                    'rdfs:label': class_name,
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'}
    }

    # determine parent class of element and add subclass relationship to schema - required by biothings
    # if no subclass is provided, set a default to schema.org Thing
    if subclass_of:
        if len(subclass_of) == 1 and pd.isnull(subclass_of[0]):
            parent = {'rdfs:subClassOf':[{'@id':'schema:Thing'}]}
        else:
            parent = {'rdfs:subClassOf':[{'@id':'bts:' + se.get_class_label_from_display_name(sub)} for sub in subclass_of]}
    else:
        parent = {'rdfs:subClassOf':[{'@id':'schema:Thing'}]}

    class_attributes.update(parent)


    # add optional attribute specifying attributes/objects that are required for the specification of this object
    # useful for specifying annotation requirements, for example
    if requires_dependencies:
        requirement = {'sms:requiresDependency':[{'@id':'bts:' + dep} for dep in requires_dependencies]}
        class_attributes.update(requirement)

    # add optional attribute specifying the possible values this object can be set to; can be other objects, including primitives 
    if requires_range:
        value_constraint = {'schema:rangeIncludes':[{'@id':'bts:' + se.get_class_label_from_display_name(val)} for val in requires_range]}
        class_attributes.update(value_constraint)

    # add optional attribute specifying validation patterns associated with this object (e.g. precise definition of the object range) 
    if validation_rules:
        class_attributes.update({'sms:validationRules': validation_rules})
    else:
        class_attributes.update({'sms:validationRules': []})

    # add optional attribute specifying the required components (i.e. high level ontology class in which entities/objects are categorized/componentized) 
    # that are required for the specification of this object
    if requires_components:
        requirement = {'sms:requiresComponent':[{'@id':'bts:' + c} for c in requires_components]}
        class_attributes.update(requirement)

    if required:
        class_attributes.update({'sms:required':'sms:true'})
    else:
        class_attributes.update({'sms:required':'sms:false'})

    # ensure display name does not contain leading/trailing white spaces
    class_attributes.update({'sms:displayName':class_display_name.strip()})

    return class_attributes


def get_property(se: SchemaExplorer, property_display_name: str, property_class_name: str, description: str = None, requires_range: list = None, requires_dependencies: list = None, required:bool = None, validation_rules: str = None) -> dict:

    """Constructs a new schema.org compliant property of an existing schema.org object/class; note that the property itself is a schema.org object class.

    Args:
        se: a schema explorer object allowing the traversal and modification of a schema graph        
        property_display_name: human readable label for the schema object/attribute: key characteristic X of the assay, related protocol, or downstream data that we want to record as metadata feature
        property_class_name: *schema* label of the class/object that this is a property of
       description: definition or a reference containing the definition of attribute X. Preferably provide a source ontology link or code in addition to the definition.
       requires_range: what is the set/domain of values that this attribute can be assigned to; currently only used to specify primitive types. TODO: extend to reg exp patterns 
       requires_dependencies: important characteristics, if any, of property X that need to be recorded as metadata features given property X is specified. These characteristics are attributes themselves and need to pre-exist in the schema as such
       validation_rules: a list of validation rules defined for this class (e.g. defining what is a valid object of this property)


    Returns: a json schema.org  property object
    """
    property_name = se.get_property_label_from_display_name(property_display_name)

    property_attributes = {
                    '@id': 'bts:' + property_name,
                    '@type': 'rdf:Property',
                    'rdfs:comment': description if description and not pd.isnull(description) else "TBD",
                    'rdfs:label': property_name,
                    'sms:displayName': property_display_name,
                    'schema:domainIncludes': {'@id': 'bts:' + se.get_class_label_from_display_name(property_class_name)},
                    'schema:isPartOf': {'@id': 'http://schema.biothings.io'},
    }
    if requires_range:
        value_constraint = {'schema:rangeIncludes':[{'@id':'bts:' + se.get_class_label_from_display_name(val)} for val in requires_range]}
        property_attributes.update(value_constraint)
    
    if requires_dependencies:
        requirement = {'sms:requiresDependency':[{'@id':'bts:' + dep} for dep in requires_dependencies]}
        property_attributes.update(requirement)

    # add optional attribute specifying validation patterns associated with this object (e.g. precise definition of the object range) 
    if validation_rules:
        property_attributes.update({'sms:validationRules': validation_rules})
    else:
        property_attributes.update({'sms:validationRules': []})

    if required:
        property_attributes.update({'sms:required':'sms:true'})
    else:
        property_attributes.update({'sms:required':'sms:false'})


    #'http://schema.org/domainIncludes':{'@id': 'bts:' + property_class_name},
    #'http://schema.org/rangeIncludes':{'@id': 'schema:' + allowed_values},
    
    # ensure display name does not contain leading/trailing white spaces
    property_attributes.update({'sms:displayName':property_display_name.strip()})
    
    return property_attributes


def attribute_exists(se: SchemaExplorer, attribute_label:str) -> bool:

    """Check if a given attribute exists already in schema

    Args:
       se: a schema explorer object allowing the traversal and modification of a schema graph        
       attribute_label: a schema label for the attribute to check

    Returns:
       True/False indicating if attribute exists or not
    """
    schema_graph = se.get_nx_schema()
    
    if attribute_label in schema_graph.nodes:
        return True
    return False


def check_schema_definition(schema_definition: pd.DataFrame) -> bool:

    """Checks if a schema definition data frame contains the right required headers.

    See schema definition guide for more details
    TODO: post and link schema definition guide
       
    Args: 
        schema_definition: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0
    Raises: Exception
    """
    
    if required_headers.issubset(set(list(schema_definition.columns))):
        return
    raise Exception()


def create_schema_classes(schema_extension: pd.DataFrame, se: SchemaExplorer) -> SchemaExplorer:
    
    """Creates classes for all attributes and adds them to the schema
    Args:
        schema_extension: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0 
        se: a schema explorer object allowing the traversal and modification of a schema graph        
        base_schema_path: a path to a json-ld file containing an existing schema

    Returns: 
        An updated schema explorer object
    """

    try:
        check_schema_definition(schema_extension)
        print("Schema definition csv ready for processing!")
    except:
        print("Schema extension headers: ")     
        print(set(list(schema_extension.columns)))
        print("do not match required schema headers: ")
        print(required_headers)
        print("ERROR: could not add extension to schema!")
        exit()

    # get attributes from Attribute column
    attributes = schema_extension[list(required_headers)].to_dict("records")
    
    # get all properties across all attributes from Properties column
    props = set(schema_extension[["Properties"]].dropna().values.flatten())

    # clean properties strings
    all_properties = []
    for prop in props:
        all_properties += [p.strip() for p in prop.split(",")]

    # get both attributes and their properties (if any)
    properties = schema_extension[["Attribute", "Properties"]].to_dict("records")

    # property to class map
    prop_2_class = {}
    for record in properties:
        if not pd.isnull(record["Properties"]):
            props = record["Properties"].strip().split(",")
            for p in props:
                prop_2_class[p.strip()] = record["Attribute"]

    print("=====================")
    print("Adding attributes")
    print("=====================")
    for attribute in attributes:

        required = None
        if not pd.isnull(attribute["Required"]):
            required = attribute["Required"]


        if not attribute["Attribute"] in all_properties:
            display_name = attribute["Attribute"]
           
            subclass_of = None
            if not pd.isnull(attribute["Parent"]):
                subclass_of = [parent for parent in attribute["Parent"].strip().split(",")]

            new_class = get_class(se, display_name,
                                        description = attribute["Description"],
                                        subclass_of = subclass_of,
                                        required = required
            )
            
            se.update_class(new_class)

            """
            print(se.get_nx_schema().nodes[new_class["rdfs:label"]])

            # check if attribute doesn't already exist and add it
            if not attribute_exists(se, new_class["rdfs:label"]):
                se.update_class(new_class)
            else:
                print("ATTRIBUTE EXISTS")
                print(new_class)
            """

        else:
            display_name = attribute["Attribute"]

            new_property = get_property(se, display_name,
                                        prop_2_class[display_name],
                                        description = attribute["Description"],
                                        required = required
            )
            # check if attribute doesn't already exist and add it
            if not attribute_exists(se, new_property["rdfs:label"]):
                se.update_property(new_property)
    
    print("=====================")
    print("Done adding attributes")
    print("=====================")

    #TODO check if schema already contains property - may require property context in csv schema definition

    print("=====================")
    print("Adding and editing properties")
    print("=====================")

    for prop in properties:
        if not pd.isnull(prop["Properties"]): # a class may have or not have properties
            for p in prop["Properties"].strip().split(","): # a class may have multiple properties
                attribute = prop["Attribute"]

                # check if property is already present as attribute under attributes column
                # TODO: adjust logic below to compactify code
                p = p.strip()
                if p in list(schema_extension["Attribute"]):
                    description = schema_extension.loc[schema_extension["Attribute"] == p]["Description"].values[0]
                    property_info = se.explore_property(se.get_property_label_from_display_name(p))
                    range_values = property_info["range"] if "range" in property_info else None
                    requires_dependencies = property_info["dependencies"] if "dependencies" in property_info else None
                    required = property_info["required"] if "required" in property_info else None
                                    
                    new_property = get_property(se, p,
                                                property_info["domain"],
                                                description = description,
                                                requires_range = range_values,
                                                requires_dependencies = requires_dependencies,
                                                required = required
                    )
                    se.edit_property(new_property)
                else: 
                    description = None
                    new_property = get_property(se, p,
                                                attribute,
                                                description = description
                    ) 
                    se.update_property(new_property)

    print("=====================")
    print("Done adding properties")
    print("=====================")

    # set range values and dependency requirements for each attribute
    # if not already added, add each attribute in required values and dependencies to the schema extension
    print("=====================")
    print("Editing attributes and properties to add requirements and value ranges")
    print("=====================")

    for attribute in attributes:

        # TODO: refactor processing of multi-valued cells in columns and corresponding schema updates; it would compactify code below if class and property are encapsulated as objects inheriting from a common attribute parent object

        # get values in range for this attribute, if any are specified
        range_values = attribute["Valid Values"]
        if not pd.isnull(range_values):
            print(">>> Adding value range for " + attribute["Attribute"])

            # prepare the range values list and split based on appropriate delimiter
            # if the string "range_values" starts with double quotes, then extract all "valid values" within double quotes
            range_values_list = []
            if range_values[0] == "\"":
                range_values_list = re.findall(r'"([^"]*)"', range_values)
            else:
                range_values_list = range_values.strip().split(",")

            for val in range_values_list:
                # check if value is in attributes column; add it as a class if not
                if not val.strip() in list(schema_extension["Attribute"]):

                    #determine parent class of the new value class
                    # if this attribute is not a property, set it as a parent class
                    if not attribute["Attribute"] in all_properties:
                        parent = attribute["Attribute"]
                    else:
                    # this attribute is a property, set the parent to the domain class of this attribute
                        parent = se.get_class_by_property(attribute["Attribute"])
                        if not parent:
                            print("ERROR: Listed valid value " + val + " for attribute " + attribute["Attribute"] + " must have a class parent!")
                            print("Could not add extension to schema!")
                            exit()
                    
                    new_class = get_class(se, val,
                                        description = None,
                                        subclass_of = [parent]
                    )
                    # check if attribute doesn't already exist and add it
                    if not attribute_exists(se, new_class["rdfs:label"]):
                        se.update_class(new_class)
                                    
                #update rangeIncludes of attribute
                # if attribute is not a property, then assume it is a class
                if not attribute["Attribute"] in all_properties:
                    print(attribute["Attribute"])
                    class_info = se.explore_class(se.get_class_label_from_display_name(attribute["Attribute"]))
                    class_info["range"].append(se.get_class_label_from_display_name(val))
                    class_range_edit = get_class(se, attribute["Attribute"],
                                                description = attribute["Description"],\
                                                subclass_of = [attribute["Parent"]],\
                                                requires_dependencies = class_info["dependencies"],\
                                                requires_range = class_info["range"],
                                                required = class_info["required"],
                                                validation_rules = class_info["validation_rules"] 
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
                                                    requires_range = property_info["range"],
                                                    required = property_info["required"],
                                                    validation_rules = property_info["validation_rules"]
                    )
                    se.edit_property(property_range_edit)
                print(val + " added to value range.")            
            
            print("<<< Done adding value range for " + attribute["Attribute"])

        # get validation rules for this attribute, if any are specified
        validation_rules = attribute["Validation Rules"]
        if not pd.isnull(validation_rules):
            print(">>> Adding validation rules for " + attribute["Attribute"])

            # TODO: make validation rules delimiter configurable parameter
            validation_rules = [val_rule.strip() for val_rule in validation_rules.strip().split("::")]

            #update validation rules of attribute
            # if attribute is not a property, then assume it is a class
            if not attribute["Attribute"] in all_properties:
                print(attribute["Attribute"])
                class_info = se.explore_class(se.get_class_label_from_display_name(attribute["Attribute"]))
                class_info["validation_rules"] = validation_rules
                class_val_rule_edit = get_class(se, attribute["Attribute"],
                                                description = attribute["Description"],\
                                                subclass_of = [attribute["Parent"]],\
                                                requires_dependencies = class_info["dependencies"],\
                                                requires_range = class_info["range"],
                                                required = class_info["required"],
                                                validation_rules = class_info["validation_rules"] 
                )
                se.edit_class(class_val_rule_edit)
            else:
            # the attribute is a property
                property_info = se.explore_property(se.get_property_label_from_display_name(attribute["Attribute"]))
                property_info["validation_rules"] = validation_rules  
                property_val_rule_edit = get_property(se, attribute["Attribute"],
                                                   property_info["domain"],
                                                   description = property_info["description"],
                                                   requires_dependencies = property_info["dependencies"],
                                                   requires_range = property_info["range"],
                                                   required = property_info["required"],
                                                   validation_rules = property_info["validation_rules"]
                )
                se.edit_property(property_val_rule_edit)


        # get dependencies for this attribute, if any are specified
        requires_dependencies = attribute["Requires"]
        if not pd.isnull(requires_dependencies):
            print(">>> Adding dependencies for " + attribute["Attribute"])

            for dep in requires_dependencies.strip().split(","):
                # check if dependency is a property or not
                dep = dep.strip()
                dep_is_property = dep in all_properties
                dep_label = ""
                # set dependency label based on kind of dependency: class or property 
                if dep_is_property:
                    dep_label = se.get_property_label_from_display_name(dep) 
                else:
                    dep_label = se.get_class_label_from_display_name(dep)

              
                # check if dependency is in attributes column; add it to the list if not
                if not dep.strip() in list(schema_extension["Attribute"]):
                    # if dependency is a property create a new property; else create a new class
                    if not dep_is_property:
                        # if this attribute is not a property, set it as a parent class
                        if not attribute["Attribute"] in all_properties:
                            parent = attribute["Attribute"]
                        else:
                        # this attribute is a property, set the parent to the domain class of this attribute
                            parent = se.get_class_by_property(attribute["Attribute"])
                            if not parent:
                                print("ERROR: Listed required dependency " + dep + "for attribute " + attribute["Attribute"] + " must have a class parent!")
                                print("Could not add extension to schema!")
                                exit()

                        new_class = get_class(se, dep,
                                              description = None,
                                              subclass_of = [parent]
                        )
                        #se.update_class(new_class)
                        # check if attribute doesn't already exist and add it
                        if not attribute_exists(se, new_class["rdfs:label"]):
                            se.update_class(new_class)

                    else:
                        if not attribute["Attribute"] in all_properties:
                            domain_attribute = attribute["Attribute"]
                        else:
                        # this attribute is a property, set the domain of this property to the domain class of the attribute
                            domain_attribute = se.get_class_by_property(attribute["Attribute"])
                            if not domain_attribute:
                                print("ERROR: Listed required dependency " + dep + " must have a class parent!")
                                print("Could not add extension to schema!")
                                exit()

                        description = None
                        new_property = get_property(se, dep,
                                                    domain_attribute,
                                                    description = description
                        )
                        # check if attribute doesn't already exist and add it
                        if not attribute_exists(se, new_property["rdfs:label"]):
                            se.update_property(new_property)

                # update required dependencies of attribute
                # if attribute is not a property then assume it is a class
                if not attribute["Attribute"] in all_properties:
                    class_info = se.explore_class(se.get_class_label_from_display_name(attribute["Attribute"]))
                    class_info["dependencies"].append(dep_label)
                    class_dependencies_edit = get_class(se, attribute["Attribute"],
                                                description = attribute["Description"],
                                                subclass_of = [attribute["Parent"]],
                                                requires_dependencies = class_info["dependencies"], 
                                                requires_range = class_info["range"],
                                                required = class_info["required"],
                                                validation_rules = class_info["validation_rules"]
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
                                                        requires_range = property_info["range"],
                                                        required = property_info["required"],
                                                        validation_rules = property_info["validation_rules"]
                    )
                    se.edit_property(property_dependencies_edit)
                print(dep + " added to dependencies.")

            #TODO check for cycles in attribute dependencies schema subgraph

            print("<<< Done adding dependencies for " + attribute["Attribute"])


        # check if the attribute requires any components
        if not pd.isnull(attribute["Requires Component"]):
            component_dependencies = attribute["Requires Component"] 
        else: 
            continue

        print(">>> Adding component dependencies for " + attribute["Attribute"])

        # iterate over potentially multiple dependency components
        for comp_dep in component_dependencies.strip().split(","):

            # check if a component is already defined as an attribute; if not define it in the schema
            if not comp_dep.strip() in list(schema_extension["Attribute"]):

                #component is not in csv schema so try adding it as a class with a parent Thing
                new_class = get_class(se, comp_dep,
                                      description = None
                )
                
                # check if attribute doesn't already exist in schema.org schema and add it
                # (component may not be in csv schema, but could be in the base schema we are extending)
                if not attribute_exists(se, new_class["rdfs:label"]):
                    se.update_class(new_class)
        
            #update this attribute requirements to include component
            class_info = se.explore_class(se.get_class_label_from_display_name(attribute["Attribute"]))
            class_info["component_dependencies"].append(se.get_class_label_from_display_name(comp_dep))
            class_component_dependencies_edit = get_class(se, attribute["Attribute"],
                                            description = class_info["description"],
                                            subclass_of = class_info["subClassOf"],
                                            requires_dependencies = class_info["dependencies"], 
                                            requires_range = class_info["range"],
                                            validation_rules = class_info["validation_rules"],
                                            requires_components = class_info["component_dependencies"]
            )
            se.edit_class(class_component_dependencies_edit)


        #TODO check for cycles in component dependencies schema subgraph

        print("<<< Done adding component dependencies for " + attribute["Attribute"])


    print("=====================")
    print("Done adding requirements and value ranges to attributes")
    print("=====================")


    return se
