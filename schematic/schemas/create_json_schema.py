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
class Node:
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
    name: str = ""
    display_name: str = ""
    valid_values: list[str] = field(default_factory=list)
    valid_value_display_names: list[str] = field(default_factory=list)
    validation_rules: list[str] = field(default_factory=list)
    is_required: bool = False
    dependencies: list[str] = field(default_factory=list)
    dependency_display_names: list[str] = field(default_factory=list)
    description: str = "TBD"
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
        raise ValueError(
            "Found more than one inRange rule in validation rules: ", rule_list
        )
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
        raise ValueError(
            "Found more than one type rule in validation rules: ", rule_list
        )
    if len(type_rules) == 0:
        return None
    return type_rules[0]


@dataclass
class NodeProcessor:
    """
    This is a helper class for JSONSchemaGenerator. It creates a JSON Schema for an input datatype.
    This datatype is the source node. A graph(nx.MultiDiGraph) created from the data model
    is processed for valid_values and reverse dependencies in order to create conditional
    dependencies in the resulting JSON Schema. This class keeps track of the states needed as
    the graph is processed.

    Attributes:
        dmge: A DataModelGraphExplorer for the graph
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
    dmge: DataModelGraphExplorer
    source_node: str
    root_dependencies: list[str] = field(init=False)
    nodes_to_process: list[str] = field(init=False)
    current_node: Optional[Node] = field(init=False)
    processed_nodes: list[str] = field(default_factory=list)
    reverse_dependencies: dict[str, list[str]] = field(default_factory=dict)
    valid_values_map: dict[str, list[str]] = field(default_factory=dict)
    dmr: DataModelRelationships = field(init=False)


    def __post_init__(self) -> None:
        """
        The first nodes to process are the root dependencies.
        This sets the current node as the first node in root dependencies.
        """
        self.dmr = DataModelRelationships()
        root_dependencies = sorted(self.dmge.get_adjacent_nodes_by_relationship(
            node_label=self.source_node,
            relationship=self.dmr.relationships_dictionary["requiresDependency"]["edge_key"],
        ))
        # If root_dependencies is empty it means that a class with name 'source_node' exists
        # in the schema, but it is not a valid component
        if not root_dependencies:
            raise ValueError(f"'{self.source_node}' is not a valid datatype in the data model.")
        self.root_dependencies = root_dependencies
        self.nodes_to_process = self.root_dependencies.copy()
        self.current_node = None
        self.move_to_next_node()


    def move_to_next_node(self) -> None:
        """Removes the first node in nodes to process and sets it as current node"""
        if self.nodes_to_process:
            current_node = self.nodes_to_process.pop(0)
            display_name = self._get_node_display_name(current_node)
            valid_values=self._get_node_valid_values(current_node)
            valid_value_display_names=self.dmge.get_nodes_display_names(valid_values)
            validation_rules= self.dmge.get_component_node_validation_rules(
                manifest_component=self.source_node, node_display_name=display_name
            )
            is_required = self.dmge.get_component_node_required(
                manifest_component=self.source_node,
                node_validation_rules=validation_rules,
                node_display_name=display_name
            )
            dependencies = self.dmge.get_adjacent_nodes_by_relationship(
                node_label=current_node,
                relationship=self.dmr.relationships_dictionary["requiresDependency"]["edge_key"],
            )
            dependency_display_names = self.dmge.get_nodes_display_names(
                node_list=dependencies
            )
            description = self.dmge.get_node_comment(node_display_name=display_name)
            self.current_node = Node(
                name=current_node,
                display_name=display_name,
                valid_values=valid_values,
                valid_value_display_names=valid_value_display_names,
                validation_rules=validation_rules,
                is_required=is_required,
                dependencies=dependencies,
                dependency_display_names=dependency_display_names,
                description=description
            )
            self.update_reverse_dependencies(
                display_name, dependency_display_names
            )
            self.update_valid_values_map(
                current_node, valid_values
            )
            self.update_nodes_to_process(sorted(valid_values))
            self.update_nodes_to_process(sorted(dependencies))
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
        return self.current_node.name in self.processed_nodes

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
        if self.current_node.name is None:
            raise ValueError("Current node is None")
        self.processed_nodes.append(self.current_node.name)

    def is_current_node_a_property(self) -> bool:
        return any(
            [
                self.current_node.display_name in self.reverse_dependencies,
                self.current_node.is_required,
                self.current_node.name in self.root_dependencies,
            ]
        )

    def is_current_node_in_reverse_dependencies(self) -> bool:
        return self.current_node.display_name in self.reverse_dependencies

    def _get_node_display_name(self, node:str) -> str:
        node_list = self.dmge.get_nodes_display_names([node])
        if not node_list:
            raise ValueError("node missing form graph: ", node)
        return node_list[0]

    def _get_node_valid_values(self, node:str) -> list[str]:
        return self.dmge.get_adjacent_nodes_by_relationship(
            node_label=node,
            relationship=self.dmr.relationships_dictionary["rangeIncludes"]["edge_key"],
        )


def create_json_schema(
    dmge: DataModelGraphExplorer,
    datatype: str,
    schema_name: str,
    write_schema: bool = True,
    schema_path: Union[str, None] = None,
    jsonld_path: Optional[str] = None
) -> dict[str, Any]:
    """
    Creates a JSONSchema dict for the datatype in the data model.

    This uses the input graph starting at the node that corresponds to the input datatype.
    Starting at the given node it will(recursively):
    1) Find all the nodes / terms this node depends on (which are required as
        "additional metadata" given this node is "required").
    2) Find all the allowable metadata values / nodes that can be assigned to a particular
        node (if such a constraint is specified on the schema).

    Using the above data it will:
    - Cerate properties for each attribute of the datatype.
    - Create properties for attributes that are conditionally
        dependent on the datatypes attributes
    - Create conditional dependencies linking attributes to their dependencies

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
    node_processor = NodeProcessor(dmge, datatype)

    json_schema = JSONSchema(
        schema_id="http://example.com/" + schema_name,
        title=schema_name,
        description=dmge.get_node_comment(node_label=datatype),
    )

    while node_processor.are_nodes_remaining():
        if not node_processor.is_current_node_processed():
            _process_node(json_schema, node_processor)
        node_processor.move_to_next_node()

    logger.info("JSON schema successfully generated from schema.org schema!")

    json_schema_dict = json_schema.as_json_schema_dict()

    if write_schema:
        _write_data_model(json_schema_dict, schema_path, datatype, jsonld_path)

    return json_schema_dict

def _process_node(json_schema: JSONSchema, node_processor: NodeProcessor) -> None:
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

    if node_processor.is_current_node_a_property():
        # Determine if current node has conditional dependencies that need to be set
        if node_processor.is_current_node_in_reverse_dependencies():
            _set_conditional_dependencies(json_schema=json_schema, np=node_processor)
            # This is to ensure that all properties that are conditional dependencies are not
            #   required, but only become required when the conditional dependency is met.
            node_processor.current_node.is_required = False
        _set_property(json_schema, node_processor.current_node)
        node_processor.update_processed_nodes_with_current_node()


def _write_data_model(
    json_schema_dict: dict[str, Any],
    schema_path: Optional[str] = None,
    name: Optional[str] = None,
    jsonld_path: Optional[str] = None,
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
    elif name and jsonld_path:
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
    else:
        raise ValueError(
            "Either schema_path or both name and jsonld_path must be provided."
        )
    export_json(json_doc=json_schema_dict, file_path=json_schema_path, indent=2)


def _set_conditional_dependencies(
    json_schema: JSONSchema,
    np:NodeProcessor,
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
        np: A NodeProcessor that is on the current node
    """
    # The enum is the specific value that triggers the conditional dependency.
    # When the watched_property == enum, the conditional_property(current_node) will become required
    for enum in np.reverse_dependencies[np.current_node.display_name]:
        if enum in np.valid_values_map:
            properties = sorted(np.valid_values_map[enum])
            for watched_property in properties:
                conditional_schema = {
                    "if": {"properties": {watched_property: {"enum": [enum]}}},
                    "then": {
                        "properties": {np.current_node.name: {"not": {"type": "null"}}},
                        "required": [np.current_node.name],
                    },
                }
                json_schema.add_to_all_of_list(conditional_schema)


def _set_property(json_schema: JSONSchema, node: Node) -> None:
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
    if node.valid_value_display_names:
        if node.is_array:
            schema_property = _create_enum_array_property(node)
        else:
            schema_property = _create_enum_property(node)

    else:
        if node.is_array:
            schema_property = _create_array_property(node)
        else:
            schema_property = _create_simple_property(node)

    json_schema.update_property(schema_property)

    if node.is_required:
        json_schema.add_required_property(node.name)


def _create_enum_array_property(node: Node) -> dict[str, Any]:
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
    types = [{"type": "array", "title": "array", "items": {"enum": sorted(node.valid_value_display_names)}}]

    if not node.is_required:
        types += [{"type": "null", "title": "null"}]

    schema = {node.name: {"oneOf": types, "description": node.description}}
    return schema  # type: ignore


def _create_array_property(node: Node) -> dict[str, Any]:
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

    items: dict[str, Any] = {}
    if node.type:
        items["type"] = node.type
    if node.minimum is not None:
        items["minimum"] = node.minimum
    if node.maximum is not None:
        items["maximum"] = node.maximum

    if items:
        array_dict["items"] = items

    types = [array_dict]
    if not node.is_required:
        types.append({"type": "null", "title": "null"})

    schema = {node.name: {"oneOf": types, "description": node.description}}
    return schema


def _create_enum_property(node: Node) -> dict[str, Any]:
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
    schema: dict[str, Any] = {node.name: {"description": node.description}}
    one_of_list: list[dict[str, Any]] = [{"enum": sorted(node.valid_value_display_names), "title": "enum"}]
    if not node.is_required:
        one_of_list += [{"type": "null", "title": "null"}]
    schema[node.name]["oneOf"] = one_of_list
    return schema


def _create_simple_property(node: Node) -> dict[str, Any]:
    """
    Creates a JSON Schema property
    If a property_type is given the type is added to the schema
    If a property_type is not given and is_required is  "not: {type:null}" is added
      to the schema

    Arguments:
        name: What to name the object
        property_data: Info parsed about the property from the validation rules
        is_required: Whether or not the property is required
        description: a description of the property

    Returns:
        JSON object
    """
    schema: dict[str, Any] = {node.name: {"description": node.description}}

    if node.type:
        if node.is_required:
            schema[node.name]["type"] = node.type
        else:
            schema[node.name]["oneOf"] = [
                {"type": node.type, "title": node.type},
                {"type": "null", "title": "null"},
            ]
    elif node.is_required:
        schema[node.name]["not"] = {"type": "null"}

    if node.minimum is not None:
        schema[node.name]["minimum"] = node.minimum
    if node.maximum is not None:
        schema[node.name]["maximum"] = node.maximum

    return schema
