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
                #if node == 'Patient' and attribute_display_name == 'HTAN Participant ID' and csv_header == 'Parent':
                #    breakpoint()
                # For a given relationship in the model
                if csv_header in relationships.keys():
                    # if the current node is part of that relationship and is not the current node
                    if node in relationships[csv_header] and node != attribute_display_name:
                        #print('Creating edge relationship \"' + csv_header +'\" with node ' + node + ' and attribute ' + attribute_display_name)
                        # Connect node to attribute as an edge.
                        G.add_edge(all_node_dict[node]['label'], all_node_dict[attribute_display_name]['label'], key=key)
                        # Add additional valid value edges
                        if key == 'rangeIncludes':
                            G.add_edge(all_node_dict[attribute_display_name]['label'], all_node_dict[node]['label'],  key='subClassOf')

        return G

    def edit_edge():
        return