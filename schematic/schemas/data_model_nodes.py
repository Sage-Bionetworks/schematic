class DataModelNodes():
    def __init__(self):
        return

    def node_present(self, G, node_name):
        if node_name in G.nodes():
            return True
        else:
            return False

    def gather_nodes(self, attr_info):
        """Take in a tuple containing attriute name and relationship dictionary, and find all nodes defined in attribute information.
        Args:

        Returns:
            list, nodes defined by attribure_info as being related to that attribute.
        """
        rel_w_nodes = [
                    "Valid Values",
                    "DependsOn",
                    "Parent",
                    "Properties",
                    "DependsOn Component",
                    ]
        attribute, relationship = attr_info
        relationship = relationship['Relationships']

        nodes = []
        if attribute not in nodes:
            nodes.append(attribute)
        for rel in rel_w_nodes:
            if rel in relationship.keys():
                nodes.extend([node.strip()
                                for node in relationship[rel]])
        return nodes

    def gather_all_nodes(self, data_model):
        """

        """
        all_nodes = []
        for attr_info in data_model.items():
            nodes = self.gather_nodes(attr_info=attr_info)
            all_nodes.extend(nodes)
        all_nodes = [*set(all_nodes)]
        return all_nodes

    def generate_node(self, G, all_nodes, data_model):
        """
        """
        return

    def edit_node():
        return