"""Data model Nodes"""

from typing import Optional, Callable, Any

from inspect import isfunction
import networkx as nx  # type: ignore
from rdflib import Namespace

from schematic.schemas.data_model_relationships import DataModelRelationships

from schematic.utils.schema_utils import (
    get_label_from_display_name,
    get_attribute_display_name_from_label,
    convert_bool_to_str,
    parse_validation_rules,
    DisplayLabelType,
    EntryType,
)


class DataModelNodes:
    """Data model Nodes"""

    def __init__(self, attribute_relationships_dict: dict):
        self.namespaces = {
            "rdf": Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        }
        self.data_model_relationships = DataModelRelationships()
        self.value_relationships = (
            self.data_model_relationships.retreive_rel_headers_dict(edge=False)
        )
        self.edge_relationships_dictionary = (
            self.data_model_relationships.retreive_rel_headers_dict(edge=True)
        )
        self.properties = self.get_data_model_properties(
            attr_rel_dict=attribute_relationships_dict
        )
        # retrieve a list of relationship types that will produce nodes.
        self.node_relationships = list(self.edge_relationships_dictionary.values())

    def gather_nodes(self, attr_info: tuple) -> list:
        """
        Take in a tuple containing attriute name and relationship dictionary,
          and find all nodes defined in attribute information.

        Args:
            attr_info, tuple: (Display Name, Relationships Dictionary portion of
              attribute_relationships dictionary)

        Returns:
            nodes, list: nodes related to the given node (specified in attr_info).
        Note:
            Extracting nodes in this fashion ensures order is preserved.
        """

        # Extract attribute and relationship dictionary
        attribute, relationship = attr_info
        relationships = relationship["Relationships"]

        nodes = []
        if attribute not in nodes:
            nodes.append(attribute)
        for rel in self.node_relationships:
            if rel in relationships.keys():
                nodes.extend([node for node in relationships[rel] if node is not None])
        return nodes

    def gather_all_nodes_in_model(self, attr_rel_dict: dict) -> list:
        """Gather all nodes in the data model, in order.
        Args:
            attr_rel_dict, dict: generated in data_model_parser
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Returns:
            all_nodes, list: List of all node display names in the data model
              preserving order entered.
        Note:
            Gathering nodes in this fashion ensures order is preserved.
        """
        all_nodes = []
        for attr_info in attr_rel_dict.items():
            nodes = self.gather_nodes(attr_info=attr_info)
            all_nodes.extend(nodes)
        # Remove any duplicates preserving order
        all_nodes = list(dict.fromkeys(all_nodes).keys())
        return all_nodes

    def get_rel_node_dict_info(self, relationship: str) -> Optional[tuple[str, dict]]:
        """For each display name get defaults for nodes.
        Args:
            relationship, str: relationship key to match.
        Returns:
            rel_key, str: relationship node label
            rel_node_dict, dict: node_attr_dict, from relationships dictionary for a
              given relationship
        TODO: Move to data_model_relationships.
        """
        for (
            key,
            value,
        ) in self.data_model_relationships.relationships_dictionary.items():
            if key == relationship:
                if "node_attr_dict" in value:
                    rel_key = value["node_label"]
                    rel_node_dict = value["node_attr_dict"]
                    return rel_key, rel_node_dict
        return None

    def get_data_model_properties(self, attr_rel_dict: dict) -> list:
        """Identify all properties defined in the data model.
        Args:
            attr_rel_dict, dict:
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Returns:
            properties,list: properties defined in the data model
        """
        properties = []
        for relationships in attr_rel_dict.values():
            if "Properties" in relationships["Relationships"].keys():
                properties.extend(relationships["Relationships"]["Properties"])
        properties = list(set(properties))
        return properties

    def get_entry_type(self, node_display_name: str) -> EntryType:
        """Get the entry type of the node, property or class.

        Args:
            node_display_name (str): display name of target node.

        Returns:
            EntryType: returns 'property' or 'class' based on data model specifications.
        """
        if node_display_name in self.properties:
            entry_type: EntryType = "property"
        else:
            entry_type = "class"
        return entry_type

    def run_rel_functions(
        self,
        rel_func: Callable,
        node_display_name: str = "",
        key: str = "",
        attr_relationships: Optional[dict] = None,
        csv_header: str = "",
        entry_type: EntryType = "class",
        data_model_labels: DisplayLabelType = "class_label",
    ) -> Any:
        """
        This function exists to centralzie handling of functions for filling out node information,
          makes sure all the proper parameters are passed to each function.

        Args:
            rel_func, callable: Function to call to get information to attach to the node
            node_display_name, str: node display name
            key, str: relationship key
            attr_relationships, dict: relationships portion of attributes_relationships dictionary
            csv_header, str: csv header
            entry_type, str: 'class' or 'property' defines how

        Returns:
            Outputs of specified rel_func (relationship function)

        For legacy:
        elif key == 'id' and rel_func == get_label_from_display_name:
            func_output = get_label_from_display_name(
                display_name =node_display_name, entry_type=entry_type
            )
        """
        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-return-statements
        # pylint: disable=comparison-with-callable
        if attr_relationships is None:
            attr_relationships = {}

        if rel_func == get_attribute_display_name_from_label:
            return get_attribute_display_name_from_label(
                node_display_name, attr_relationships
            )

        if rel_func == parse_validation_rules:
            rules = attr_relationships[csv_header]
            if isinstance(rules, (dict, list)):
                return parse_validation_rules(rules)

        if rel_func == get_label_from_display_name:
            return get_label_from_display_name(
                display_name=node_display_name,
                entry_type=entry_type,
                data_model_labels=data_model_labels,
            )

        if rel_func == convert_bool_to_str:
            if isinstance(attr_relationships[csv_header], str):
                if attr_relationships[csv_header].lower() == "true":
                    return True
                if attr_relationships[csv_header].lower() == "false":
                    return False
                return None

            if isinstance(attr_relationships[csv_header], bool):
                return attr_relationships[csv_header]

            return None

        # Raise Error if the rel_func provided is not captured.
        raise ValueError(
            (
                f"The function provided ({rel_func}) to define the relationship {key} "
                "is not captured in the function run_rel_functions, please update."
            )
        )

    def generate_node_dict(
        self,
        node_display_name: str,
        attr_rel_dict: dict,
        data_model_labels: DisplayLabelType = "class_label",
    ) -> dict:
        """Gather information to be attached to each node.

        Note:
            If the default calls function, call that function for the default or alternate
              implementation.
            May need to update this logic for varying function calls. (for example the current
              function takes in the node display name would need to update if new function took
              in something else.)

        Args:
            node_display_name (str): display name for current node
            attr_rel_dict (dict): generated in data_model_parser
              {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
            data_model_labels (DisplayLabelType, optional):str, display_label or class_label.
                display_label, use the display name as a label, if it is valid (contains no
                  blacklisted characters) otherwise will default to schema_label.
                class_label, default, use standard class or property label.

        Returns:
            dict: dictionary of relationship information about the current node
                {'displayName': '', 'label': '', 'comment': 'TBD', 'required': None,
                 'validationRules': [], 'isPartOf': '', 'uri': ''}
        """
        # Strip whitespace from node display name
        node_display_name = node_display_name.strip()

        # Determine if property or class
        entry_type = self.get_entry_type(node_display_name=node_display_name)

        # If the node is an attribute, find its relationships.
        attr_relationships = {}
        if node_display_name in attr_rel_dict.keys():
            attr_relationships = attr_rel_dict[node_display_name]["Relationships"]

        # Initialize node_dict
        node_dict = {}

        # Look through relationship types that represent values (i.e. do not define edges)
        for key, csv_header in self.value_relationships.items():
            # Get key and defalt values current relationship type.
            rel_node = self.get_rel_node_dict_info(key)
            assert rel_node is not None
            rel_key, rel_node_dict = rel_node

            # If we have information to add about this particular node, get it
            if csv_header in attr_relationships.keys():
                # Check if the 'standard' specifies calling a function.
                if "standard" in rel_node_dict.keys() and isfunction(
                    rel_node_dict["standard"]
                ):
                    # Add to node_dict The value comes from the standard function call.
                    node_dict.update(
                        {
                            rel_key: self.run_rel_functions(
                                rel_node_dict["standard"],
                                node_display_name=node_display_name,
                                key=key,
                                attr_relationships=attr_relationships,
                                csv_header=csv_header,
                                entry_type=entry_type,
                                data_model_labels=data_model_labels,
                            )
                        }
                    )
                else:
                    # For standard entries, get information from attr_relationship dictionary
                    node_dict.update({rel_key: attr_relationships[csv_header]})
            # else, add default values
            else:
                # Check if the default specifies calling a function.
                if "default" in rel_node_dict.keys() and isfunction(
                    rel_node_dict["default"]
                ):
                    node_dict.update(
                        {
                            rel_key: self.run_rel_functions(
                                rel_node_dict["default"],
                                node_display_name=node_display_name,
                                key=key,
                                attr_relationships=attr_relationships,
                                csv_header=csv_header,
                                entry_type=entry_type,
                                data_model_labels=data_model_labels,
                            )
                        }
                    )
                else:
                    # Set value to defaults.
                    node_dict.update({rel_key: rel_node_dict["default"]})

        return node_dict

    def generate_node(self, graph: nx.MultiDiGraph, node_dict: dict) -> nx.MultiDiGraph:
        """Create a node and add it to the networkx multidigraph being built
        Args:
            graph, nx.MultiDigraph: networkx multidigraph object, that is in the process of
              being fully built.
            node_dict, dict: dictionary of relationship information about the current node
        Returns:
            nx.MultiDigraph: networkx multidigraph object, that has had an additional
              node added to it.
        """
        graph.add_node(node_dict["label"], **node_dict)
        return graph

    def edit_node(self) -> None:
        """Stub for future node editor."""
        return
