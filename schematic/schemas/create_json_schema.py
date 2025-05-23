"""
This module contains create_json_schema class, and its helper functions, and support classes.
create_json_schema traverses a graph crated from the data model.
The GraphTraversalState class keeps track of the status of this traversal
The Node class gets all the information about a node in the graph needed to write a property
The JSONSchema class is used to store all the data needed to write the final JSON Schema
"""

import logging
import os
from typing import Union, Any, Optional
from dataclasses import dataclass, field, asdict

from schematic.schemas.data_model_graph import DataModelGraphExplorer
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

# Complex types
Items = dict[str, Union[str, float, list[str]]]
Property = dict[str, Union[str, float, list, dict]]
TypeDict = dict[str, Union[str, Items]]
AllOf = dict[str, Any]


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
    properties: dict[str, Property] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    all_of: list[AllOf] = field(default_factory=list)

    def as_json_schema_dict(
        self,
    ) -> dict[str, Union[str, dict[str, Property], list[str], list[AllOf]]]:
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
        if not self.all_of:
            json_schema_dict.pop("allOf")
        return json_schema_dict

    def add_required_property(self, name: str) -> None:
        """
        Adds a property to the required list

        Arguments:
            name: The name of the property
        """
        self.required.append(name)

    def add_to_all_of_list(self, item: AllOf) -> None:
        """
        Adds a property to the all_of list

        Arguments:
            item: The item to add to the all_of list
        """
        self.all_of.append(item)

    def update_property(self, property_dict: dict[str, Property]) -> None:
        """
        Updates the property dict

        Raises:
            ValueError: If the property dict has more than one key
            ValueError: If the property dict is empty
            ValueError: if the property dict key match a property that already exists

        Arguments:
            property_dict: The property dict to add to the properties dict
        """
        keys = list(property_dict.keys())
        if len(keys) > 1:
            raise ValueError(
                f"Attempting to add property dict with more than one key: {property_dict}"
            )
        if len(keys) == 0:
            raise ValueError(f"Attempting to add empty property dict: {property_dict}")
        if keys[0] in self.properties:
            raise ValueError(
                f"Attempting to add property that already exists: {property_dict}"
            )
        self.properties.update(property_dict)


@dataclass
class Node:  # pylint: disable=too-many-instance-attributes
    """
    A Dataclass representing data about a node in a data model in graph form
    A DataModelGraphExplorer is used to infer most of the fields from the name of the node

    Attributes:
        name: The name of the node
        source_node: The name of the node where the graph traversal started
        dmge: A DataModelGraphExplorer with the data model loaded
        display_name: The display name of the node
        valid_values: The valid values of the node if any
        valid_value_display_names: The display names of the valid values of the node if any
        is_required: Whether or not this node is required
        dependencies: This nodes dependencies
        description: This nodes description, gotten from the comment in the data model
        type: The type of the property (inferred from validation_rules)
        is_array: Whether or not the property is an array (inferred from validation_rules)
        minimum: The minimum value of the property (if numeric) (inferred from validation_rules)
        maximum: The maximum value of the property (if numeric) (inferred from validation_rules)
    """

    name: str
    source_node: str
    dmge: DataModelGraphExplorer
    display_name: str = field(init=False)
    valid_values: list[str] = field(init=False)
    valid_value_display_names: list[str] = field(init=False)
    is_required: bool = field(init=False)
    dependencies: list[str] = field(init=False)
    description: str = field(init=False)
    type: Optional[str] = field(init=False)
    is_array: bool = field(init=False)
    minimum: Optional[float] = field(init=False)
    maximum: Optional[float] = field(init=False)

    def __post_init__(self) -> None:
        """
        Uses the dmge to fill in most of the fields of the dataclass

        Raises:
            ValueError: If the type is not numeric, and there is an
              inRange rule in the validation rules
        """
        self.display_name = self.dmge.get_nodes_display_names([self.name])[0]
        self.valid_values = sorted(self.dmge.get_node_range(node_label=self.name))
        self.valid_value_display_names = sorted(
            self.dmge.get_node_range(node_label=self.name, display_names=True)
        )
        validation_rules = self.dmge.get_component_node_validation_rules(
            manifest_component=self.source_node, node_display_name=self.display_name
        )
        self.is_required = self.dmge.get_component_node_required(
            manifest_component=self.source_node,
            node_validation_rules=validation_rules,
            node_display_name=self.display_name,
        )
        self.dependencies = sorted(
            self.dmge.get_node_dependencies(
                self.name, display_names=False, schema_ordered=False
            )
        )
        self.description = self.dmge.get_node_comment(
            node_display_name=self.display_name
        )

        self.type = None
        self.is_array = False
        self.minimum = None
        self.maximum = None

        if validation_rules:
            if rule_in_rule_list("list", validation_rules):
                self.is_array = True

            type_rule = _get_type_rule_from_rule_list(validation_rules)
            if type_rule:
                self.type = TYPE_RULES.get(type_rule)

            range_rule = _get_in_range_rule_from_rule_list(validation_rules)
            if range_rule:
                if self.type is None:
                    self.type = "number"
                elif self.type not in ["number", "integer"]:
                    raise ValueError(
                        "Validation type must be either 'number' or 'integer' "
                        f"when using the inRange rule, but got: {self.type}"
                    )
                self.minimum, self.maximum = _get_ranges_from_range_rule(range_rule)


def _get_ranges_from_range_rule(
    rule: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Returns the min and max from an inRange rule if they exist

    Arguments:
        rule: The inRange rule

    Returns:
        The min and max from the rule
    """
    range_min: Union[float, None] = None
    range_max: Union[float, None] = None
    parameters = rule.split(" ")
    if len(parameters) > 1 and parameters[1].isnumeric():
        range_min = float(parameters[1])
    if len(parameters) > 2 and parameters[2].isnumeric():
        range_max = float(parameters[2])
    return (range_min, range_max)


def _get_in_range_rule_from_rule_list(rule_list: list[str]) -> Optional[str]:
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


def _get_type_rule_from_rule_list(rule_list: list[str]) -> Optional[str]:
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
class GraphTraversalState:  # pylint: disable=too-many-instance-attributes
    """
    This is a helper class for create_json_schema. It keeps track of the state as the function
    traverses a graph made from a data model.

    Attributes:
        dmge: A DataModelGraphExplorer for the graph
        source_node: The name of the node where the graph traversal started
        current_node: The node that is being processed
        _root_dependencies: The nodes the source node depends on
        _nodes_to_process: The nodes that are left to be processed
        _processed_nodes: The nodes that have already been processed
        _reverse_dependencies:
            Some nodes will have reverse dependencies (nodes that depend on them)
            This is a mapping: {"node_name" : [reverse_dependencies]}
        _valid_values_map:
            Some nodes will have valid_values (enums)
            This is a mapping {"valid_value" : [nodes_that_have_valid_value]}
    """

    dmge: DataModelGraphExplorer
    source_node: str
    current_node: Optional[Node] = field(init=False)
    _root_dependencies: list[str] = field(init=False)
    _nodes_to_process: list[str] = field(init=False)
    _processed_nodes: list[str] = field(init=False)
    _reverse_dependencies: dict[str, list[str]] = field(init=False)
    _valid_values_map: dict[str, list[str]] = field(init=False)

    def __post_init__(self) -> None:
        """
        The first nodes to process are the root dependencies.
        This sets the current node as the first node in root dependencies.
        """
        self.current_node = None
        self._processed_nodes = []
        self._reverse_dependencies = {}
        self._valid_values_map = {}
        root_dependencies = sorted(
            self.dmge.get_node_dependencies(
                self.source_node, display_names=False, schema_ordered=False
            )
        )
        if not root_dependencies:
            raise ValueError(
                f"'{self.source_node}' is not a valid datatype in the data model."
            )
        self._root_dependencies = root_dependencies
        self._nodes_to_process = self._root_dependencies.copy()
        self.move_to_next_node()

    def move_to_next_node(self) -> None:
        """Removes the first node in nodes to process and sets it as current node"""
        if self._nodes_to_process:
            node_name = self._nodes_to_process.pop(0)
            self.current_node = Node(
                name=node_name, dmge=self.dmge, source_node=self.source_node
            )
            self._update_valid_values_map(
                self.current_node.name, self.current_node.valid_values
            )
            self._update_reverse_dependencies(
                self.current_node.name,
                self.current_node.dependencies,
            )
            self._update_nodes_to_process(sorted(self.current_node.valid_values))
            self._update_nodes_to_process(sorted(self.current_node.dependencies))
        else:
            self.current_node = None

    def are_nodes_remaining(self) -> bool:
        """
        Determines if there are any nodes left to process

        Returns:
            Whether or not there are any nodes left to process
        """
        return self.current_node is not None

    def is_current_node_processed(self) -> bool:
        """
        Determines if  the current node has been processed yet

        Raises:
            ValueError: If there is no current node

        Returns:
            Whether or not the current node has been processed yet
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        return self.current_node.name in self._processed_nodes

    def is_current_node_a_property(self) -> bool:
        """
        Determines if the current node should be written as a property

        Raises:
            ValueError: If there is no current node

        Returns:
            Whether or not the current node should be written as a property
        """
        if self.current_node is None:
            raise ValueError("Current node is None")

        return any(
            [
                self.current_node.name in self._reverse_dependencies,
                self.current_node.is_required,
                self.current_node.name in self._root_dependencies,
            ]
        )

    def is_current_node_in_reverse_dependencies(self) -> bool:
        """
        Determines if the current node is in the reverse dependencies

        Raises:
            ValueError: If there is no current node

        Returns:
            Whether or not the current node is in the reverse dependencies
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        return self.current_node.name in self._reverse_dependencies

    def update_processed_nodes_with_current_node(self) -> None:
        """
        Adds the current node to the list of processed nodes

        Raises:
            ValueError: If there is no current node
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        self._processed_nodes.append(self.current_node.name)

    def get_conditional_properties(
        self, use_node_display_names: bool = True
    ) -> list[tuple[str, str]]:
        """Returns the conditional dependencies for the current node

        Raises:
            ValueError: If there is no current node

        Arguments:
            use_node_display_names: If True the the attributes in the
              conditional dependencies are return with their display names

        Returns:
            The watched_property, and the value for it that triggers the condition
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        conditional_properties: list[tuple[str, str]] = []
        for value in self._reverse_dependencies[self.current_node.name]:
            if value in self._valid_values_map:
                properties = sorted(self._valid_values_map[value])
                for watched_property in properties:
                    if use_node_display_names:
                        watched_property = self.dmge.get_nodes_display_names(
                            [watched_property]
                        )[0]
                        value = self.dmge.get_nodes_display_names([value])[0]
                    conditional_properties.append((watched_property, value))
        return conditional_properties

    def _update_valid_values_map(
        self, node_display_name: str, valid_values_display_names: list[str]
    ) -> None:
        """Updates the valid_values map

        Arguments:
            node_display_name: The display name of the node
            valid_values_display_names: The display names of the the nodes valid values
        """
        for node in valid_values_display_names:
            if node not in self._valid_values_map:
                self._valid_values_map[node] = []
            self._valid_values_map[node].append(node_display_name)

    def _update_reverse_dependencies(
        self, node_display_name: str, node_dependencies_display_names: list[str]
    ) -> None:
        """Updates the reverse dependencies

        Arguments:
            node_display_name: The display name of the node
            node_dependencies_display_names: the display names of the reverse dependencies
        """
        for dep in node_dependencies_display_names:
            if dep not in self._reverse_dependencies:
                self._reverse_dependencies[dep] = []
            self._reverse_dependencies[dep].append(node_display_name)

    def _update_nodes_to_process(self, nodes: list[str]) -> None:
        """Updates the nodes to process with the input nodes

        Arguments:
            nodes: Nodes to add
        """
        self._nodes_to_process += nodes


def create_json_schema(  # pylint: disable=too-many-arguments
    dmge: DataModelGraphExplorer,
    datatype: str,
    schema_name: str,
    write_schema: bool = True,
    schema_path: Union[str, None] = None,
    jsonld_path: Optional[str] = None,
    use_property_display_names: bool = True,
    use_valid_value_display_names: bool = True,
) -> dict[str, Any]:
    """
    Creates a JSONSchema dict for the datatype in the data model.

    This uses the input graph starting at the node that corresponds to the input datatype.
    Starting at the given node it will(recursively):
    1. Find all the nodes this node depends on
    2. Find all the allowable metadata values / nodes that can be assigned to a particular
        node (if such a constraint is specified on the schema).

    Using the above data it will:
    - Cerate properties for each attribute of the datatype.
    - Create properties for attributes that are conditionally
        dependent on the datatypes attributes
    - Create conditional dependencies linking attributes to their dependencies

    Arguments:
        dmge: A DataModelGraphExplorer with the data model loaded
        datatype: the datatype to create the schema for.
            Its node is where we can start recursive dependency traversal
            (as mentioned above).
        schema_name: Name assigned to JSON-LD schema (to uniquely identify it via URI
            when it is hosted on the Internet).
        write_schema: whether or not to write the schema as a json file
        schema_path: Where to save the JSON Schema file
        jsonld_path: Used to name the file if the path isn't supplied
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names

    Returns:
        JSON Schema as a dictionary.
    """
    logger.info("Starting to create JSON Schema for %s", datatype)
    graph_state = GraphTraversalState(dmge, datatype)

    json_schema = JSONSchema(
        schema_id="http://example.com/" + schema_name,
        title=schema_name,
        description=dmge.get_node_comment(node_label=datatype),
    )

    while graph_state.are_nodes_remaining():
        if not graph_state.is_current_node_processed():
            _process_node(
                json_schema=json_schema,
                graph_state=graph_state,
                use_property_display_names=use_property_display_names,
                use_valid_value_display_names=use_valid_value_display_names,
            )
        graph_state.move_to_next_node()

    logger.info("JSON schema successfully created for %s", datatype)

    json_schema_dict = json_schema.as_json_schema_dict()

    if write_schema:
        _write_data_model(json_schema_dict, schema_path, datatype, jsonld_path)

    return json_schema_dict


def _process_node(
    json_schema: JSONSchema,
    graph_state: GraphTraversalState,
    use_property_display_names: bool = True,
    use_valid_value_display_names: bool = True,
) -> None:
    """
    Processes a node in the data model graph.
    If it should be a property in the JSON Schema, that is set.
    If it is a property with reverse dependencies, conditional properties are set.

    Argument:
        json_schema: The JSON Scheme where the node might be set as a property
        graph_state: The instance tracking the current state of the graph
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names
    """
    if graph_state.current_node is None:
        raise ValueError("Node Processor contains no node.")
    logger.info("Processing node %s", graph_state.current_node.name)

    if graph_state.is_current_node_a_property():
        # Determine if current node has conditional dependencies that need to be set
        if graph_state.is_current_node_in_reverse_dependencies():
            _set_conditional_dependencies(
                json_schema=json_schema,
                graph_state=graph_state,
                use_property_display_names=use_property_display_names,
            )
            # This is to ensure that all properties that are conditional dependencies are not
            #   required, but only become required when the conditional dependency is met.
            graph_state.current_node.is_required = False
        _set_property(
            json_schema=json_schema,
            node=graph_state.current_node,
            use_property_display_names=use_property_display_names,
            use_valid_value_display_names=use_valid_value_display_names,
        )
        graph_state.update_processed_nodes_with_current_node()
        logger.info("Property set in JSON Schema for %s", graph_state.current_node.name)


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
    else:
        raise ValueError(
            "Either schema_path or both name and jsonld_path must be provided."
        )
    export_json(json_doc=json_schema_dict, file_path=json_schema_path, indent=2)
    logger.info("The JSON schema has been saved at %s", json_schema_path)


def _set_conditional_dependencies(
    json_schema: JSONSchema,
    graph_state: GraphTraversalState,
    use_property_display_names: bool = True,
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
        json_schema: The JSON Scheme where the node might be set as a property
        graph_state: The instance tracking the current state of the graph
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
    """
    if graph_state.current_node is None:
        raise ValueError("Node Processor contains no node.")

    if use_property_display_names:
        node_name = graph_state.current_node.display_name
    else:
        node_name = graph_state.current_node.name

    conditional_properties = graph_state.get_conditional_properties(
        use_property_display_names
    )
    for prop in conditional_properties:
        attribute, value = prop
        conditional_schema = {
            "if": {"properties": {attribute: {"enum": [value]}}},
            "then": {
                "properties": {node_name: {"not": {"type": "null"}}},
                "required": [node_name],
            },
        }
        json_schema.add_to_all_of_list(conditional_schema)


def _set_property(
    json_schema: JSONSchema,
    node: Node,
    use_property_display_names: bool = True,
    use_valid_value_display_names: bool = True,
) -> None:
    """
    Sets a property in the JSON schema. that is required by the schema

    Arguments:
        json_schema: The JSON Scheme where the node might be set as a property
        graph_state: The node the write the property for
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names
    """
    if use_property_display_names:
        node_name = node.display_name
    else:
        node_name = node.name

    if node.valid_values:
        if node.is_array:
            prop = _create_enum_array_property(node, use_valid_value_display_names)
        else:
            prop = _create_enum_property(node, use_valid_value_display_names)

    else:
        if node.is_array:
            prop = _create_array_property(node)
        else:
            prop = _create_simple_property(node)

    prop["description"] = node.description
    schema_property = {node_name: prop}

    json_schema.update_property(schema_property)

    if node.is_required:
        json_schema.add_required_property(node_name)


def _create_enum_array_property(
    node: Node, use_valid_value_display_names: bool = True
) -> Property:
    """
    Creates a JSON Schema property array with enum items

    Example:
        {
            "oneOf": [
                {
                    "type": "array",
                    "title": "array",
                    "items": {"enum": ["enum1"]},
                }
            ],
        },


    Arguments:
        node: The node to make the property of
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names

    Returns:
        JSON object
    """
    if use_valid_value_display_names:
        valid_values = node.valid_value_display_names
    else:
        valid_values = node.valid_values
    items: Items = {"enum": valid_values}
    types = [
        {
            "type": "array",
            "title": "array",
            "items": items,
        }
    ]

    if not node.is_required:
        types += [{"type": "null", "title": "null"}]

    enum_array_property: Property = {"oneOf": types}
    return enum_array_property  # type: ignore


def _create_array_property(node: Node) -> Property:
    """
    Creates a JSON Schema property array

    Example:
        {
            "oneOf": [
                {
                    "type": "array",
                    "title": "array",
                    "items": {"type": "number", "minimum": 0, "maximum": 1},
                }
            ],
        }

    Arguments:
        node: The node to make the property of

    Returns:
        JSON object
    """

    items: Items = {}
    if node.type:
        items["type"] = node.type
    if node.minimum is not None:
        items["minimum"] = node.minimum
    if node.maximum is not None:
        items["maximum"] = node.maximum

    array_type_dict: TypeDict = {"type": "array", "title": "array"}
    null_type_dict: TypeDict = {"type": "null", "title": "null"}

    if items:
        array_type_dict["items"] = items

    types = [array_type_dict]
    if not node.is_required:
        types.append(null_type_dict)

    array_property: Property = {"oneOf": types}
    return array_property


def _create_enum_property(
    node: Node, use_valid_value_display_names: bool = True
) -> Property:
    """
    Creates a JSON Schema property enum

    Example:
        {
            "oneOf": [
                {"enum": ["enum1"], "title": "enum"},
                {"type": "null", "title": "null"},
            ],
        }

    Arguments:
        node: The node to make the property of

    Returns:
        JSON object
    """
    if use_valid_value_display_names:
        valid_values = node.valid_value_display_names
    else:
        valid_values = node.valid_values

    enum_property: Property = {}
    one_of_list = [{"enum": valid_values, "title": "enum"}]
    if not node.is_required:
        one_of_list += [{"type": "null", "title": "null"}]
    enum_property["oneOf"] = one_of_list

    return enum_property


def _create_simple_property(node: Node) -> Property:
    """
    Creates a JSON Schema property

    Example:
        {
            "oneOf": [
                {"type": "string", "title": "string"},
                {"type": "null", "title": "null"},
            ],
        }

    Arguments:
        node: The node to make the property of

    Returns:
        JSON object
    """
    prop: Property = {}

    if node.type:
        if node.is_required:
            prop["type"] = node.type
        else:
            prop["oneOf"] = [
                {"type": node.type, "title": node.type},
                {"type": "null", "title": "null"},
            ]
    elif node.is_required:
        prop["not"] = {"type": "null"}

    if node.minimum is not None:
        prop["minimum"] = node.minimum
    if node.maximum is not None:
        prop["maximum"] = node.maximum

    return prop
