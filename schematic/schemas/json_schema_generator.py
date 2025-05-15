"""
This module contains The JSONSchemaGenerator class, and its helper functions.
It also contains the classes Node Processor,PropertyData and JSONSchema which are used internally
 by the JSONSchemaGenerator class.
"""

import logging
import os
from typing import Union, Any, Optional
from dataclasses import dataclass, field, asdict
import networkx as nx  # type: ignore

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.schema_utils import get_json_schema_log_file_path
from schematic.utils.validate_utils import rule_in_rule_list

from schematic.utils.io_utils import export_json


logger = logging.getLogger(__name__)

# A dict where the keys are type validation rules, and the values are their JSON Schema equivalent
TYPE_RULES = {
    "str": "string",
    "num": "number",
    "float": "number",
    "int": "integer",
    "bool": "boolean",
}


@dataclass
class PropertyData:
    """
    A Dataclass representing data about a JSON Schema property from its validation rules.
    Currently Schematic infers certain data about the property from validation rules in
     the data model.
    The type is taken from type validation rules (see TYPE_RULES).
    Whether or not its an array depends on if a list rule is present.
    The maximum and minimum are taken from the inRange rule if present.

    Attributes:
      validation_rules: A list of validation rules for a property
      type: The type of the property (inferred from validation_rules)
      is_array: Whether or not the property is an array (inferred from validation_rules)
      minimum: The minimum value of the property (if numeric) (inferred from validation_rules)
      maximum: The maximum value of the property (if numeric) (inferred from validation_rules)
    """
    validation_rules: list[str] = field(default_factory=list)
    type: Optional[str] = field(init=False)
    is_array: bool = field(init=False)
    minimum: Optional[float] = field(init=False)
    maximum: Optional[float] = field(init=False)

    def __post_init__(self) -> None:
        self.type = None
        self.is_array = False
        self.minimum = None
        self.maximum = None

        if self.validation_rules:
            if rule_in_rule_list("list", self.validation_rules):
                self.is_array = True

            type_rule = _get_type_rule_from_rule_list(self.validation_rules)
            if type_rule:
                self.type = TYPE_RULES.get(type_rule)

            range_rule = _get_in_range_rule_from_rule_list(self.validation_rules)
            if range_rule:
                if self.type not in ["number", "integer"]:
                    self.type = "number"
                self.minimum, self.maximum = _get_ranges_from_range_rule(range_rule)


def _get_ranges_from_range_rule(
    rule: str,
) -> tuple[Union[float, None], Union[float, None]]:
    """
    Returns the min and max from an inRange rule if they exist

    Arguments:
        rule: The inRange rule

    Returns:
        The min and max form the rule
    """
    range_min: Union[float, None] = None
    range_max: Union[float, None] = None
    parameters = rule.split(" ")
    if len(parameters) > 1 and parameters[1].isnumeric():
        range_min = float(parameters[1])
    if len(parameters) > 2 and parameters[2].isnumeric():
        range_max = float(parameters[2])
    return (range_min, range_max)


def _get_in_range_rule_from_rule_list(rule_list: list[str]) -> Union[str, None]:
    """
    Returns the inRange rule from a list of rules if there is only one
    Returns None if there are no inRange rules

    Arguments:
        rule_list: A list of validation rules

    Raises:
        ValueError: When more than one inRange rule is found

    Returns:
        The inRange rule if one is found, or None
    """
    in_range_rules = [rule for rule in rule_list if rule.startswith("inRange")]
    if len(in_range_rules) > 1:
        raise ValueError("Found more than one inRange rule in validation rules")
    if len(in_range_rules) == 0:
        return None
    return in_range_rules[0]


def _get_type_rule_from_rule_list(rule_list: list[str]) -> Union[str, None]:
    """
    Returns the type rule from a list of rules if there is only one
    Returns None if there are no type rules

    Arguments:
        rule_list: A list of validation rules

    Raises:
        ValueError: When more than one type rule is found

    Returns:
        The type rule if one is found, or None
    """
    rule_list = [rule.split(" ")[0] for rule in rule_list]
    type_rules = [rule for rule in rule_list if rule in TYPE_RULES]
    if len(type_rules) > 1:
        raise ValueError("Found more than one type rule in validation rules")
    if len(type_rules) == 0:
        return None
    return type_rules[0]

@dataclass
class JSONSchema:  # pylint: disable=too-many-instance-attributes
    """
    A dataclass representing a JSON Schema.
    Each attribute represents a keyword in a JSON Schema.

    Attributes:
        schema_id: A URI for the schema.
        title: An optional title for this schema.
        schema: Specifies which draft of the JSON Schema standard the schema adheres to.
        type: The datatype of the schema. This will always be "object".
        description: An optional description of the object described by this schema.
        properties: A list of property schemas.
        required: A list of properties required by the schema.
        all_of: A list of conditions the schema must meet. This should be removed if empty.
    """
    schema_id: str = ""
    title: str = ""
    schema: str = "http://json-schema.org/draft-07/schema#"
    type: str = "object"
    description: str = "TBD"
    properties: dict[str, Any] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    all_of: list[dict[str, Any]] = field(default_factory=list)

    def as_json_schema_dict(self) -> dict[str, Any]:
        """
        Returns class as a JSON Schema dictionary, with proper keywords

        Returns:
            The dataclass as a dict.
        """
        json_schema_dict = asdict(self)
        keywords_to_change = {
            "schema_id": "$id",
            "schema": "$schema",
            "all_of": "allOf",
        }
        for old_word, new_word in keywords_to_change.items():
            json_schema_dict[new_word] = json_schema_dict.pop(old_word)
        if len(self.all_of) == 0:
            json_schema_dict.pop("allOf")
        return json_schema_dict

    def add_required_property(self, name: str) -> None:
        """
        Adds a property to the required list

        Args:
            name: The name of the property
        """
        self.required.append(name)

    def add_to_all_of_list(self, item: dict[str, Any]) -> None:
        """
        Adds a property to the all_of list

        Args:
            item: The item to add to the all_of list
        """
        self.all_of.append(item)

    def update_property(self, property_dict: dict[str, Any]) -> None:
        """
        Updates the property dict

        Args:
            property_dict: The property dict to add to the properties dict
        """
        self.properties.update(property_dict)


@dataclass
class NodeProcessor:
    """
    This is a helper class for JSONSchemaGenerator. It creates a JSON Schema for an input datatype.
    This datatype is the source node. A graph(nx.MultiDiGraph) created from the data model
    is processed for valid_values and reverse dependencies in order to create conditional
    dependencies in the resulting JSON Schema. This class keeps track of the states needed as
    the graph is processed.

    Attributes:
        root_dependencies: The nodes the source node depends on
        nodes_to_process: The nodes that are left to be processed
        current_node: The node that is being processed
        processed_nodes: The nodes that have already been processed
        reverse_dependencies:
            Some nodes will have reverse dependencies (nodes that depend on them)
            This is a mapping: {"node_name" : [reverse_dependencies]}
        valid_values_map:
            Some nodes will have valid_values (enums)
            This is a mapping {"valid_value" : [nodes_that_have_valid_value]}
    """
    root_dependencies: list[str]
    nodes_to_process: list[str] = field(init=False)
    current_node: Optional[str] = field(init=False)
    processed_nodes: list[str] = field(default_factory=list)
    reverse_dependencies: dict[str, list[str]] = field(default_factory=dict)
    valid_values_map: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        The first nodes to process are the root dependencies.
        This sets the current node as the first node in root dependencies.
        """
        print(self.root_dependencies)
        self.nodes_to_process = self.root_dependencies.copy()
        self.current_node = self.nodes_to_process.pop(0)

    def move_to_next_node(self) -> None:
        """Removes the first node in nodes to process and sets it as current node"""
        if self.nodes_to_process:
            self.current_node = self.nodes_to_process.pop(0)
        else:
            self.current_node = None

    def are_nodes_remaining(self) -> bool:
        """
        Returns:
            Whether or not there are any nodes left to process
        """
        return self.current_node is not None

    def is_current_node_processed(self) -> bool:
        """
        Returns:
            Whether or not the current node has been processed yet
        """
        return self.current_node in self.processed_nodes

    def update_valid_values_map(
        self, node_display_name: str, valid_values_display_names: list[str]
    ) -> None:
        """Updates the valid_values map

        Arguments:
            node_display_name: The display name of the node
            valid_values_display_names: The display names of the the nodes valid values
        """
        for node in valid_values_display_names:
            if node not in self.valid_values_map:
                self.valid_values_map[node] = []
            self.valid_values_map[node].append(node_display_name)

    def update_reverse_dependencies(
        self, node_display_name: str, node_dependencies_display_names: list[str]
    ) -> None:
        """Updates the reverse dependencies

        Arguments:
            node_display_name: The display name of the node
            node_dependencies_display_names: the display names of the reverse dependencies
        """
        for dep in node_dependencies_display_names:
            if dep not in self.reverse_dependencies:
                self.reverse_dependencies[dep] = []
            self.reverse_dependencies[dep].append(node_display_name)

    def update_nodes_to_process(self, nodes: list[str]) -> None:
        """Updates the nodes to process with the input nodes

        Arguments:
            nodes: Nodes to add
        """
        self.nodes_to_process += nodes

    def update_processed_nodes_with_current_node(self) -> None:
        """Adds the current node to the list of processed nodes"""
        if self.current_node is None:
            raise ValueError("Current node is None")
        self.processed_nodes.append(self.current_node)


class JSONSchemaGenerator:  # pylint: disable=too-few-public-methods
    "Data Model Json Schema"

    def __init__(
        self,
        jsonld_path: str,
        graph: nx.MultiDiGraph,
    ) -> None:
        self.jsonld_path = jsonld_path
        self.graph = graph
        self.dmge = DataModelGraphExplorer(self.graph)
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary

    def create_json_schema(
        self,
        datatype: str,
        schema_name: str,
        schema_path: Union[str, None] = None,
        write_schema: bool = True,
    ) -> dict[str, Any]:
        """
        Consolidated method that aims to gather dependencies and value constraints across terms
        / nodes in a schema.org schema and store them in a jsonschema /JSON Schema schema.

        It does so for any given node in the schema.org schema (recursively) using the given
          node as starting point in the following manner:
        1) Find all the nodes / terms this node depends on (which are required as
          "additional metadata" given this node is "required").
        2) Find all the allowable metadata values / nodes that can be assigned to a particular
          node (if such a constraint is specified on the schema).

        Arguments:
            datatype: the datatype to create the schema for.
              Its node is where we can start recursive dependency traversal
              (as mentioned above).
            schema_name: Name assigned to JSON-LD schema (to uniquely identify it via URI
              when it is hosted on the Internet).
            schema_path: Where to save the JSON Schema file
            write_schema: whether or not to write the schema as a json file

        Returns:
            JSON Schema as a dictionary.
        """

        # Gets the dependency nodes of the source node. These will be first nodes processed.
        root_dependencies = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=datatype,
            relationship=self.rel_dict["requiresDependency"]["edge_key"],
        )
        # if root_dependencies is empty it means that a class with name 'source_node' exists
        # in the schema, but it is not a valid component
        if not root_dependencies:
            raise ValueError(f"'{datatype}' is not a valid component in the schema.")

        node_processor = NodeProcessor(sorted(root_dependencies))

        json_schema = JSONSchema(
            schema_id="http://example.com/" + schema_name,
            title=schema_name,
            description=self.dmge.get_node_comment(node_label=datatype),
        )

        while node_processor.are_nodes_remaining():
            if not node_processor.is_current_node_processed():
                self._process_node(json_schema, datatype, node_processor)
            node_processor.move_to_next_node()

        logger.info("JSON schema successfully generated from schema.org schema!")

        json_schema_dict = json_schema.as_json_schema_dict()

        if write_schema:
            _write_data_model(json_schema_dict, schema_path, datatype, self.jsonld_path)

        return json_schema_dict

    def _process_node(
        self, json_schema: JSONSchema, source_node: str, node_processor: NodeProcessor
    ) -> None:
        """
        Processes a node in the data model graph.
        If it should be a property in the JSON Schema, that is set.
        If it is a property with reverse dependencies, conditional properties are set.

        Argument:
            json_schema: The JSON Scheme where the node might be set as a property
            source_node: The node that the JSON Schema is being created for
            node_processor: A node processor for the source node
        """
        assert node_processor.current_node
        node_range = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=node_processor.current_node,
            relationship=self.rel_dict["rangeIncludes"]["edge_key"],
        )

        node_range_display_names = self.dmge.get_nodes_display_names(
            node_list=node_range
        )

        node_dependencies = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=node_processor.current_node,
            relationship=self.rel_dict["requiresDependency"]["edge_key"],
        )

        node_display_name: str = self.graph.nodes[node_processor.current_node][
            self.rel_dict["displayName"]["node_label"]
        ]

        node_validation_rules = self.dmge.get_component_node_validation_rules(
            manifest_component=source_node, node_display_name=node_display_name
        )

        is_node_required = self.dmge.get_component_node_required(
            manifest_component=source_node,
            node_validation_rules=node_validation_rules,
            node_display_name=node_display_name,
        )

        node_description = self.dmge.get_node_comment(
            node_display_name=node_display_name
        )

        # add process node as a conditional to its dependencies
        node_dependencies_d = self.dmge.get_nodes_display_names(
            node_list=node_dependencies
        )
        node_processor.update_reverse_dependencies(
            node_display_name, node_dependencies_d
        )
        node_processor.update_valid_values_map(node_processor.current_node, node_range)

        # add nodes found as dependencies and range of this processed node
        # to the list of nodes to be processed
        node_processor.update_nodes_to_process(sorted(node_range))
        node_processor.update_nodes_to_process(sorted(node_dependencies))

        # Determine if current node is a property to set
        set_property = any(
            [
                node_display_name in node_processor.reverse_dependencies,
                is_node_required,
                node_processor.current_node in node_processor.root_dependencies,
            ]
        )
        if set_property:
            # Determine if current node has conditional dependencies that need to be set
            if node_display_name in node_processor.reverse_dependencies:
                _set_conditional_dependencies(
                    json_schema,
                    node_processor.current_node,
                    node_display_name,
                    node_processor.reverse_dependencies,
                    node_processor.valid_values_map,
                )
                is_node_required = False
            _set_property(
                json_schema,
                node_processor.current_node,
                node_range_display_names,
                node_validation_rules,
                is_node_required,
                node_description,
            )
            node_processor.update_processed_nodes_with_current_node()


def _write_data_model(
    json_schema_dict: dict[str, Any],
    schema_path: Union[str, None],
    name: str,
    jsonld_path: str,
) -> None:
    """
    Creates the JSON Schema file

    Arguments:
        json_schema_dict: The JSON schema in dict form
        schema_path: Where to save the JSON Schema file
        jsonld_path:
          The path to the JSONLD model, used to create the path
          Used if schema_path is None
        name:
          The name of the datatype(source node) the schema is being created for
          Used if schema_path is None
    """
    if schema_path:
        json_schema_path = schema_path
    if not schema_path:
        json_schema_path = get_json_schema_log_file_path(
            data_model_path=jsonld_path, source_node=name
        )
        json_schema_dirname = os.path.dirname(json_schema_path)
        if json_schema_dirname != "":
            os.makedirs(json_schema_dirname, exist_ok=True)

        logger.info(
            "The JSON schema file can be inspected by setting the following "
            "nested key in the configuration: (model > location)."
        )
    export_json(json_doc=json_schema_dict, file_path=json_schema_path, indent=2)


def _set_conditional_dependencies(
    json_schema: JSONSchema,
    conditional_property: str,
    property_display_name: str,
    reverse_dependencies: dict[str, list[str]],
    range_domain_map: dict[str, list[str]],
) -> None:
    """
    This sets conditional requirements in the "allOf" keyword.
    This is used when certain properties are required depending on the value of another property.

    For example:
      In the example data model the Patient component has the Diagnosis attribute.
      The Diagnosis attribute has valid values of ["Healthy", "Cancer"].
      The Cancer valid value is also an attribute that dependsOn on the
        attributes Cancer Type and Family History
      Cancer Type and Family History are attributes with valid values.
      Therefore: When Diagnosis == "Cancer", Cancer Type and Family History should become required

    Example conditional schema:
        "if":{
            "properties":{
               "Diagnosis":{
                  "enum":[
                     "Cancer"
                  ]
               }
            }
         },
         "then":{
            "properties":{
               "FamilyHistory":{
                  "not":{
                     "type":"null"
                  }
               }
            },
            "required":[
               "FamilyHistory"
            ]
         }

    Arguments:
        json_schema: The JSON Schema to add conditional dependencies to
        conditional_property: The name of the node to add conditional dependencies for
          In the above example this would be Cancer Type or Family History
        property_display_name: the display name of the conditional property.
        reverse_dependencies: A map of nodes and a list of their dependencies
          In the above example this would be:
          {'Family History': ['Cancer'], 'Cancer Type': ['Cancer']}
        range_domain_map: A map of nodes and a list of their range
          In the above example {'Healthy': ['Diagnosis'], 'Cancer': ['Diagnosis']}
    """
    # The conditional_value will be the specific value that triggers the conditional dependency.
    # When the watched_property == conditional_value, the conditional_property will become required
    for conditional_value in sorted(reverse_dependencies[property_display_name]):
        if conditional_value in range_domain_map:
            properties = sorted(range_domain_map[conditional_value])
            # watched_property is the property such that when the
            # watched_property == conditional_value
            # the conditional_property becomes required
            for watched_property in properties:
                conditional_schema = {
                    "if": {
                        "properties": {
                            watched_property: {"enum": sorted([conditional_value])}
                        }
                    },
                    "then": {
                        "properties": {conditional_property: {"not": {"type": "null"}}},
                        "required": [conditional_property],
                    },
                }
                json_schema.add_to_all_of_list(conditional_schema)


def _set_property(  # pylint: disable=too-many-arguments
    json_schema: JSONSchema,
    name: str,
    enum_list: list[str],
    validation_rules: list[str],
    is_required: bool,
    description: str,
) -> None:
    """
    Sets a property in the JSON schema. that is required by the schema

    Arguments:
        json_schema: The JSON Schema to modify
        name: The name of the property in the JSON Schema to set
        enum_list: A list of enums(valid values) for this property
        validation_rules: The validation rules for the property
        is_required: Whether or not the property is required
        description: a description of the property
    """
    property_data = PropertyData(validation_rules)
    if enum_list:
        if property_data.is_array:
            schema_property = _create_enum_array_property(
                name=name,
                enum_list=enum_list,
                is_required=is_required,
                description=description,
            )
        else:
            schema_property = _create_enum_property(
                name=name,
                enum_list=enum_list,
                is_required=is_required,
                description=description,
            )

    else:
        if property_data.is_array:
            schema_property = _create_array_property(
                name=name,
                property_data=property_data,
                is_required=is_required,
                description=description,
            )
        else:
            schema_property = _create_simple_property(
                name=name,
                property_data=property_data,
                is_required=is_required,
                description=description,
            )

    json_schema.update_property(schema_property)

    if is_required:
        json_schema.add_required_property(name)


def _create_enum_array_property(
    name: str, enum_list: list[str], is_required: bool, description: str
) -> dict[str, Any]:
    """
    Creates a JSON Schema array/enum

    Example(not is_required):

    {
        "type": "object",
        "properties": {
            "property_name" : {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array"
                        "items": {"enum": ["enum1", "enum2"]},
                    },
                    {
                        "type": "null"
                        "title": "null"
                    }
                ]
            }
        }
    }

    Example(is_required):

    {
        "type": "object",
        "properties": {
            "property_name" : {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array"
                        "items": {"enum": ["enum1", "enum2"]},
                    }
                ]
            }
        }
    }


    Arguments:
        enum_list: List of values that will make up the enum
        name: What to name the object
        is_required: Whether or not the property is required by the schema
        description: a description of the property

    Returns:
        JSON object
    """
    types = [{"type": "array", "title": "array", "items": {"enum": sorted(enum_list)}}]

    if not is_required:
        types += [{"type": "null", "title": "null"}]

    schema = {name: {"oneOf": types, "description": description}}
    return schema  # type: ignore


def _create_array_property(
    name: str, property_data: PropertyData, is_required: bool, description: str
) -> dict[str, Any]:
    """
    Creates a JSON Schema array

    Arguments:
        name: What to name the object
        item_type: The type of of items in the array
        is_required: Whether or not the property is required by the schema
        description: a description of the property

    Returns:
        JSON object
    """

    array_dict: dict[str, Any] = {"type": "array", "title": "array"}

    include_items = any(
        [
            property_data.type,
            property_data.minimum is not None,
            property_data.maximum is not None,
        ]
    )

    if include_items:
        array_dict["items"] = {}
        if property_data.type:
            array_dict["items"]["type"] = property_data.type
        if property_data.minimum is not None:
            array_dict["items"]["minimum"] = property_data.minimum
        if property_data.maximum is not None:
            array_dict["items"]["maximum"] = property_data.maximum

    types = [array_dict]
    if not is_required:
        types += [{"type": "null", "title": "null"}]

    schema = {name: {"oneOf": types, "description": description}}
    return schema


def _create_enum_property(
    name: str, enum_list: list[str], is_required: bool, description: str
) -> dict[str, Any]:
    """
    Creates a JSON Schema enum

    Arguments:
        enum_list: List of values that will make up the enum
        name: What to name the object
        is_required: Whether or not the property is required by the schema
        description: a description of the property

    Returns:
        JSON object
    """
    schema: dict[str, Any] = {name: {"description": description}}
    one_of_list: list[dict[str, Any]] = [{"enum": sorted(enum_list), "title": "enum"}]
    if not is_required:
        one_of_list += [{"type": "null", "title": "null"}]
    schema[name]["oneOf"] = one_of_list
    return schema


def _create_simple_property(
    name: str, property_data: PropertyData, is_required: bool, description: str
) -> dict[str, Any]:
    """
    Creates a JSON Schema property
    If a property_type is given the type is added to the schema
    If a property_type is not given and is_required is  "not: {type:null} is added

    Arguments:
        name: What to name the object
        property_data: Info parsed about the property from the validation rules
        is_required: Whether or not the property is required
        description: a description of the property

    Returns:
        JSON object
    """
    print(property_data.type)
    schema: dict[str, Any] = {name: {"description": description}}

    if property_data.type and is_required:
        schema[name]["type"] = property_data.type
    elif property_data.type:
        schema[name]["oneOf"] = [
            {"type": property_data.type, "title": property_data.type},
            {"type": "null", "title": "null"},
        ]
    elif is_required:
        schema[name]["not"] = {"type": "null"}

    if property_data.minimum is not None:
        schema[name]["minimum"] = property_data.minimum
    if property_data.maximum is not None:
        schema[name]["maximum"] = property_data.maximum

    return schema

