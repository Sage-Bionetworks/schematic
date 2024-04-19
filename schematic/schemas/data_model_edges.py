"""Data Model Edges"""
from typing import Union, Any
from schematic.schemas.data_model_relationships import DataModelRelationships


class DataModelEdges:  # pylint: disable=too-few-public-methods
    """Data Model Edges"""

    def __init__(self) -> None:
        self.dmr = DataModelRelationships()
        self.data_model_relationships = self.dmr.relationships_dictionary

    def generate_edge(  # pylint: disable=too-many-arguments
        self,
        node: str,
        all_node_dict: dict[str, dict[str, Any]],
        attr_rel_dict: dict[str, dict[str, Any]],
        edge_relationships: dict[str, str],
        edge_list: list[tuple[str, str, dict[str, Union[str, int]]]],
    ) -> list[tuple[str, str, dict[str, Union[str, int]]]]:
        """

        Generate an edge between a target node and relevant other nodes the data model.
          In short, does this current node belong to a recorded relationship in the attribute,
          relationshps dictionary.
          Go through each attribute and relationship to find where the node may be.

        Args:
            node (str): target node to look for connecting edges
            all_node_dict (dict): a dictionary containing information about all nodes in the model
                key: node display name
                value: node attribute dict, containing attributes to attach to each node.
            attr_rel_dict (dict):
                {Attribute Display Name: {--disallow-untyped-defs
                    Relationships: {
                        CSV Header: Value}
                    }
                }
            edge_relationships (dict): dict, rel_key: csv_header if the key represents a value
              relationship.
            edge_list (list): list of tuples describing the edges and the edge attributes,
              organized as (node_1, node_2, {key:edge_relationship_key, weight:int})
              At this point, the edge list will be in the process of being built.
              Adding edges from list so they will be added properly to the graph without being
              overwritten in the loop, and passing the Graph around more.


        """

        # For each attribute in the model.
        for attribute_display_name, relationship in attr_rel_dict.items():
            # Get the relationships associated with the current attribute
            relationships = relationship["Relationships"]
            # Add edge relationships one at a time
            for rel_key, csv_header in edge_relationships.items():
                # If the attribute has a relationship that matches the current edge being added
                if csv_header in relationships.keys():
                    # If the current node is part of that relationship and is not the current node
                    # Connect node to attribute as an edge.
                    if (
                        node in relationships[csv_header]
                        and node != attribute_display_name
                    ):
                        # Generate weights based on relationship type.
                        # Weights will allow us to preserve the order of entries order in the
                        # data model in later steps.
                        if rel_key == "domainIncludes":
                            # For 'domainIncludes'/properties relationship, users do not explicitly
                            # provide a list order (like for valid values, or dependsOn)
                            # so we pull the order/weight from the order of the attributes.
                            weight = list(attr_rel_dict.keys()).index(
                                attribute_display_name
                            )
                        elif isinstance(relationships[csv_header], list):
                            # For other relationships that pull in lists of values, we can
                            # explicilty pull the weight by their order in the provided list
                            weight = relationships[csv_header].index(node)
                        else:
                            # For single (non list) entries, add weight of 0
                            weight = 0
                        # Get the edge_key for the edge relationship we are adding at this step
                        edge_key = self.data_model_relationships[rel_key]["edge_key"]
                        # Add edges, in a manner that preserves directionality
                        # TODO: rewrite to use edge_dir pylint: disable=fixme
                        if rel_key in ["subClassOf", "domainIncludes"]:
                            edge_list.append(
                                (
                                    all_node_dict[node]["label"],
                                    all_node_dict[attribute_display_name]["label"],
                                    {
                                        "key": edge_key,
                                        "weight": weight,
                                    },
                                )
                            )
                        else:
                            edge_list.append(
                                (
                                    all_node_dict[attribute_display_name]["label"],
                                    all_node_dict[node]["label"],
                                    {"key": edge_key, "weight": weight},
                                )
                            )
                        # Add add rangeIncludes/valid value relationships in reverse as well,
                        # making the attribute the parent of the valid value.
                        if rel_key == "rangeIncludes":
                            edge_list.append(
                                (
                                    all_node_dict[attribute_display_name]["label"],
                                    all_node_dict[node]["label"],
                                    {"key": "parentOf", "weight": weight},
                                )
                            )
        return edge_list
