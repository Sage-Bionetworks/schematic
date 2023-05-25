class DataModelEdges():
    def __init__(self):
        return

    def generate_edge(self, G, node, all_node_dict, data_model, edge_relationships):
        """
        Args:

        Returns:

        Comment:
            How do we best capture all the relationships we will be accounting for? 
        """
        for attribute_display_name, relationship in data_model.items():
            relationships = relationship['Relationships']

            for key, val in edge_relationships.items():
                # For each relationship we are interested in
                if key in relationships.keys():
                    if node in relationships[key] and node != attribute_display_name:
                        print('Creating edge with node ' + node + ' and attribute ' + attribute_display_name)
                        G.add_edge(all_node_dict[node]['label'], all_node_dict[attribute_display_name]['label'], key=val)

        return G

    def edit_edge():
        return