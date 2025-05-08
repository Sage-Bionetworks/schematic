"Data Model Json Schema"

import json
import logging
import os
from typing import Optional, Union, Any
from dataclasses import dataclass, field, asdict

import networkx as nx  # type: ignore

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.schema_utils import get_json_schema_log_file_path
from schematic.utils.validate_utils import rule_in_rule_list

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
    """A Dataclass representing data about a JSON Schema property from its validation rules"""

    property_type: Union[str, None] = None
    is_array: bool = False
    range_min: Union[float, None] = None
    range_max: Union[float, None] = None


@dataclass
class JSONSchema:  # pylint: disable=too-many-instance-attributes
    """A dataclass representing a JSON Schema"""

    schema_id: str = ""
    title: str = ""
    schema: str = "http://json-schema.org/draft-07/schema#"
    type: str = "object"
    description: str = "TBD"
    properties: dict = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    all_of: list = field(default_factory=list)

    def as_json_schema_dict(self) -> dict[str, Any]:
        """Returns class as a JSON Schema dictionary, with proper keywords"""
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


class DataModelJSONSchema2:  # pylint: disable=too-few-public-methods
    "Data Model Json Schema"

    def __init__(
        self,
        jsonld_path: str,
        graph: nx.MultiDiGraph,
    ):
        self.jsonld_path = jsonld_path
        self.jsonld_path_root: Optional[str] = None
        self.graph = graph  # Graph would be fully made at this point.
        self.dmge = DataModelGraphExplorer(self.graph)
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary

    def get_json_validation_schema(
        self, source_node: str, schema_name: str
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
            source_node: Node from which we can start recursive dependency traversal
                (as mentioned above).
            schema_name: Name assigned to JSON-LD schema (to uniquely identify it via URI
                when it is hosted on the Internet).

        Returns:
            JsonType: JSON Schema as a dictionary.
        """
        json_schema = JSONSchema(
            schema_id="http://example.com/" + schema_name,
            title=schema_name,
            description=self.dmge.get_node_comment(node_label=source_node),
        )

        # list of nodes to be checked for dependencies, starting with the source node
        nodes_to_process: list[str] = []
        # keep of track of nodes whose dependencies have been processed
        processed_nodes: list[str] = []
        # maintain a map between conditional nodes and their dependencies
        # (reversed) -- {dependency : conditional_node}
        reverse_dependencies: dict[str, list[str]] = {}
        # maintain a map between range nodes and their domain nodes {range_value : domain_value}
        # the domain node is very likely the parentof ("parentOf" relationship) of the range node
        range_domain_map: dict[str, list[str]] = {}
        # Gets the dependency nodes of the source node. These will be first nodes processed.
        root_dependencies = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=source_node,
            relationship=self.rel_dict["requiresDependency"]["edge_key"],
        )
        # if root_dependencies is empty it means that a class with name 'source_node' exists
        # in the schema, but it is not a valid component
        if not root_dependencies:
            raise ValueError(f"'{source_node}' is not a valid component in the schema.")
        # Loads the source nodes dependency nodes as first to process
        nodes_to_process += root_dependencies
        node_being_processed = nodes_to_process.pop(0)

        while node_being_processed:
            if not node_being_processed in processed_nodes:
                self._process_node(
                    json_schema,
                    node_being_processed,
                    source_node,
                    nodes_to_process,
                    processed_nodes,
                    range_domain_map,
                    root_dependencies,
                    reverse_dependencies,
                )

            # if the list of nodes to process is not empty
            # set the process node the next remaining node to process
            if nodes_to_process:
                node_being_processed = nodes_to_process.pop(0)
            else:
                # no more nodes to process
                # exit the loop
                break

        logger.info("JSON schema successfully generated from schema.org schema!")

        json_schema_dict = json_schema.as_json_schema_dict()

        _write_data_model(self.jsonld_path, source_node, json_schema_dict)

        return json_schema_dict

    def _process_node(  # pylint: disable=too-many-arguments, too-many-locals
        self,
        json_schema: JSONSchema,
        node_being_processed: str,
        source_node: str,
        nodes_to_process: list[str],
        processed_nodes: list[str],
        range_domain_map: dict[str, list[str]],
        root_dependencies: list[str],
        reverse_dependencies: dict[str, list[str]],
    ):
        # Node range(valid values, enum values)
        node_range = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=node_being_processed,
            relationship=self.rel_dict["rangeIncludes"]["edge_key"],
        )

        node_range_display_names = self.dmge.get_nodes_display_names(
            node_list=node_range
        )

        # When a datatype(component) depends on another datatype
        node_dependencies = self.dmge.get_adjacent_nodes_by_relationship(
            node_label=node_being_processed,
            relationship=self.rel_dict["requiresDependency"]["edge_key"],
        )

        node_display_name: str = self.graph.nodes[node_being_processed][
            self.rel_dict["displayName"]["node_label"]
        ]

        # updating map between node and node's valid values
        for node in node_range_display_names:
            if not node in range_domain_map:
                range_domain_map[node] = []
            range_domain_map[node].append(node_display_name)

        node_validation_rules = self.dmge.get_component_node_validation_rules(
            manifest_component=source_node, node_display_name=node_display_name
        )

        node_required = self.dmge.get_component_node_required(
            manifest_component=source_node,
            node_validation_rules=node_validation_rules,
            node_display_name=node_display_name,
        )

        if node_display_name in reverse_dependencies:
            _set_conditional_dependencies(
                json_schema, node_display_name, reverse_dependencies, range_domain_map
            )
            node_required = False

        set_property = any(
            [
                node_display_name in reverse_dependencies,
                node_required,
                node_being_processed in root_dependencies,
            ]
        )
        node_comment = self.dmge.get_node_comment(node_display_name=node_display_name)

        if set_property:
            _set_property(
                json_schema,
                node_display_name,
                node_range_display_names,
                node_validation_rules,
                node_required,
                node_comment,
            )
            node_is_processed = True
        else:
            node_is_processed = False
            # node doesn't have conditionals and it is not required and it
            # is not a root dependency the node doesn't belong in the schema
            # do not add to processed nodes since its conditional may be traversed
            # at a later iteration (though unlikely for most schemas we consider)

        # add process node as a conditional to its dependencies
        node_dependencies_d = self.dmge.get_nodes_display_names(
            node_list=node_dependencies
        )

        for dep in node_dependencies_d:
            if not dep in reverse_dependencies:
                reverse_dependencies[dep] = []

            reverse_dependencies[dep].append(node_display_name)

        # add nodes found as dependencies and range of this processed node
        # to the list of nodes to be processed
        nodes_to_process += node_range
        nodes_to_process += node_dependencies

        # if the node is processed add it to the processed nodes set
        if node_is_processed:
            processed_nodes.append(node_being_processed)


def _write_data_model(
    jsonld_path: str, name: str, json_schema_dict: dict[str, Any]
) -> None:
    """
    Creates the JSON Schema file

    Arguments:
        jsonld_path: The path to the JSONLD model, used to create the path
        name: The name of the datatype(source node) the schema is being created for
        json_schema_dict: The JSON schema in dict form
    """
    json_schema_log_file_path = get_json_schema_log_file_path(
        data_model_path=jsonld_path, source_node=name
    )
    json_schema_dirname = os.path.dirname(json_schema_log_file_path)
    if json_schema_dirname != "":
        os.makedirs(json_schema_dirname, exist_ok=True)
    with open(json_schema_log_file_path, "w", encoding="UTF-8") as js_f:
        json.dump(json_schema_dict, js_f, indent=2)


def _set_conditional_dependencies(
    json_schema: JSONSchema,
    conditional_property: str,
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
               "Family History":{
                  "not":{
                     "type":"null"
                  }
               }
            },
            "required":[
               "Family History"
            ]
         }

    Arguments:
        json_schema: The JSON Schema to add conditional dependencies to
        conditional_property: The name of the node to add conditional dependencies for
          In the above example this would be Cancer Type or Family History
        reverse_dependencies: A map of nodes and a list of their dependencies
          In the above example this would be:
          {'Family History': ['Cancer'], 'Cancer Type': ['Cancer']}
        range_domain_map: A map of nodes and a list of their range
          In the above example {'Healthy': ['Diagnosis'], 'Cancer': ['Diagnosis']}
    """
    # The conditional_value will be the specific value that triggers the conditional dependency.
    # When the watched_property == conditional_value, the conditional_property will become required
    for conditional_value in reverse_dependencies[conditional_property]:
        if conditional_value in range_domain_map:
            properties = range_domain_map[conditional_value]
            # watched_property is the property such that when the
            # watched_property == conditional_value
            # the conditional_property becomes required
            for watched_property in properties:
                conditional_schema = {
                    "if": {
                        "properties": {watched_property: {"enum": [conditional_value]}}
                    },
                    "then": {
                        "properties": {conditional_property: {"not": {"type": "null"}}},
                        "required": [conditional_property],
                    },
                }
                json_schema.all_of.append(conditional_schema)


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
    property_data = _get_property_data_from_validation_rules(validation_rules)
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

    json_schema.properties.update(schema_property)

    if is_required:
        json_schema.required += [name]


def _create_enum_array_property(
    name: str, enum_list: list[str], is_required: bool, description: str
) -> dict[str, Any]:
    """
    Creates a JSON Schema array/enum

    Arguments:
        enum_list: List of values that will make up the enum
        name: What to name the object
        is_required: Whether or not the property is required by the schema
        description: a description of the property

    Returns:
        JSON object
    """
    types = [{"type": "array", "items": {"enum": enum_list}}]

    if not is_required:
        types += [{"type": "null"}]

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

    array_dict: dict = {"type": "array"}

    include_items = any(
        [
            property_data.property_type,
            property_data.range_min is not None,
            property_data.range_max is not None,
        ]
    )

    if include_items:
        array_dict["items"] = {}
        if property_data.property_type:
            array_dict["items"]["type"] = property_data.property_type
        if property_data.range_min is not None:
            array_dict["items"]["minimum"] = property_data.range_min
        if property_data.range_max is not None:
            array_dict["items"]["maximum"] = property_data.range_max

    types = [array_dict]
    if not is_required:
        types += [{"type": "null"}]

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
    if not is_required:
        enum_list += [None]  # type: ignore
    return {name: {"enum": enum_list, "description": description}}


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
    schema: dict = {name: {"description": description}}

    if property_data.property_type and is_required:
        schema[name]["type"] = property_data.property_type
    elif property_data.property_type:
        schema[name]["type"] = [property_data.property_type, "null"]
    elif is_required:
        schema[name]["not"] = {"type": "null"}

    if property_data.range_min is not None:
        schema[name]["minimum"] = property_data.range_min
    if property_data.range_max is not None:
        schema[name]["maximum"] = property_data.range_max

    return schema


def _get_property_data_from_validation_rules(
    validation_rules: list[str],
) -> PropertyData:
    """
    Returns data from validation rules in the form of a PropertyData dataclass

    Args:
        validation_rules: A list of validation rules

    Returns:
        PropertyData: validation rule data
    """
    property_type: Union[str, None] = None
    is_array = False
    range_min: Union[float, None] = None
    range_max: Union[float, None] = None

    if validation_rules:
        if rule_in_rule_list("list", validation_rules):
            is_array = True

        type_rule = _get_type_rule_from_rule_list(validation_rules)
        if type_rule:
            property_type = TYPE_RULES.get(type_rule)

        range_rule = _get_in_range_rule_from_rule_list(validation_rules)
        if range_rule:
            if property_type not in ["number", "integer"]:
                property_type = "number"
            range_min, range_max = _get_ranges_from_range_rule(range_rule)

    return PropertyData(property_type, is_array, range_min, range_max)


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
