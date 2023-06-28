class DataModelEdges():
    def __init__(self):
        return

    def generate_edge(self, G, node, all_node_dict, data_model, edge_relationships):
        """
        Args:

        Returns:
        """
        # For each attribute in the model.
        for attribute_display_name, relationship in data_model.items():
            # Get the relationships for the current attribure
            relationships = relationship['Relationships']
            # For each edge relationship
            for key, csv_header in edge_relationships.items():
                # For a given relationship in the model
                if csv_header in relationships.keys():
                    # If the current node is part of that relationship and is not the current node
                    # Connect node to attribute as an edge.
                    if node in relationships[csv_header] and node != attribute_display_name: 
                        # Find position of node in the list, this is the weight
                        # This will help us ensure things like valid values, or depends on are preserved in the proper order.
                        
                        # TODO: Move adding weights to its own helper.
                        # TODO: create a new attribute in the rel dictionary looking for directionality. Save as out for domainIncludes, save as in for others.
                        if key == 'domainIncludes':
                            # Get weight from the order of the attributes.
                            weight = list(data_model.keys()).index(attribute_display_name)
                        elif type(relationships[csv_header]) == list:
                            weight = relationships[csv_header].index(node)
                        else:
                            weight = 0
                        # Here the first added node to the edge is the value that would be the valid value to the second node which is the attribute.
                        G.add_edge(all_node_dict[node]['label'], all_node_dict[attribute_display_name]['label'], key=key, weight=weight)
                        # Add additional valid value edges
                        if key == 'rangeIncludes':
                            # Add this relationships for classes.
                            G.add_edge(all_node_dict[attribute_display_name]['label'], all_node_dict[node]['label'],  key='subClassOf', weight=weight)

        return G

    def edit_edge():
        return