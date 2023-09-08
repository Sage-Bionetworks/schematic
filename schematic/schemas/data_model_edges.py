import networkx as nx

from schematic.schemas.data_model_relationships import (
    DataModelRelationships
    )

class DataModelEdges():
    def __init__(self):
        self.dmr = DataModelRelationships()
        self.data_model_relationships = self.dmr.relationships_dictionary

    def generate_edge(self, G: nx.MultiDiGraph, node: str, all_node_dict: dict, attr_rel_dict: dict, edge_relationships: dict) -> nx.MultiDiGraph:
        """Generate an edge between a target node and relevant other nodes the data model
        Args:
            G, nx.MultiDiGraph: networkx graph representation of the data model, that is in the process of being fully built.
            node, str: target node to look for connecting edges
            all_node_dict, dict: a dictionary containing information about all nodes in the model
                key: node display name
                value: node attribute dict, containing attributes to attach to each node.
            attr_rel_dict, dict:
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
            edge_relationships: dict, key: csv_header if the key represents a value relationship.

        Returns:
            G, nx.MultiDiGraph: networkx graph representation of the data model, that has had new edges attached.
        """
        # For each attribute in the model.
        for attribute_display_name, relationship in attr_rel_dict.items():
            # Get the relationships associated with the current attribute
            relationships = relationship['Relationships']
            # Add edge relationships one at a time
            for key, csv_header in edge_relationships.items():
                # If the attribute has a relationship that matches the current edge being added
                if csv_header in relationships.keys():
                    # If the current node is part of that relationship and is not the current node
                    # Connect node to attribute as an edge.
                    if node in relationships[csv_header] and node != attribute_display_name: 
                        # Generate weights based on relationship type. 
                        # Weights will allow us to preserve the order of entries order in the data model in later steps.
                        if key == 'domainIncludes':
                            # For 'domainIncludes'/properties relationship, users do not explicitly provide a list order (like for valid values, or dependsOn)
                            # so we pull the order/weight from the order of the attributes.
                            weight = list(attr_rel_dict.keys()).index(attribute_display_name)
                        elif type(relationships[csv_header]) == list:
                            # For other relationships that pull in lists of values, we can explicilty pull the weight by their order in the provided list
                            weight = relationships[csv_header].index(node)
                        else:
                            # For single (non list) entries, add weight of 0
                            weight = 0
                        # Get the edge_key for the edge relationship we are adding at this step
                        edge_key = self.data_model_relationships[key]['edge_key']
                        
                        # Add edges, in a manner that preserves directionality
                        # TODO: rewrite to use edge_dir
                        if key in ['subClassOf', 'domainIncludes']:
                            G.add_edge(all_node_dict[node]['label'], all_node_dict[attribute_display_name]['label'], key=edge_key, weight=weight)
                        else:
                            G.add_edge(all_node_dict[attribute_display_name]['label'], all_node_dict[node]['label'], key=edge_key, weight=weight)
                        # Add add rangeIncludes/valid value relationships in reverse as well, making the attribute the parent of the valid value.
                        if key == 'rangeIncludes':
                            G.add_edge(all_node_dict[attribute_display_name]['label'], all_node_dict[node]['label'],  key='parentOf', weight=weight)

        return G
