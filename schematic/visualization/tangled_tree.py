from io import StringIO
import json
import logging
import networkx as nx
import numpy as np
import os
from os import path
import pandas as pd

# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text, List

from schematic.utils.viz_utils import visualize
from schematic.visualization.attributes_explorer import AttributesExplorer
from schematic.schemas.explorer import SchemaExplorer
from schematic.schemas.generator import SchemaGenerator
from schematic import LOADER
from schematic.utils.io_utils import load_json
from copy import deepcopy

# Make sure to have newest version of decorator


logger = logging.getLogger(__name__)
#OUTPUT_DATA_DIR = str(Path('tests/data/visualization/AMPAD').resolve())
#DATA_DIR = str(Path('tests/data').resolve())

class TangledTree(object):
    """
    """

    def __init__(self,
                 path_to_json_ld: str,
                 figure_type: str,
                 ) -> None:
        # Load jsonld
        self.path_to_json_ld = path_to_json_ld
        self.json_data_model = load_json(self.path_to_json_ld)

        # Parse schema name
        self.schema_name = path.basename(self.path_to_json_ld).split(".model.jsonld")[0]

        # Instantiate a schema generator to retrieve db schema graph from metadata model graph
        self.sg = SchemaGenerator(self.path_to_json_ld)      

        # Get metadata model schema graph
        self.G = self.sg.se.get_nx_schema()

        # Set Parameters
        self.figure_type = figure_type.lower()
        self.dependency_type = ''.join(('requires', self.figure_type.capitalize()))

        # Get names
        self.schema = load_json(self.path_to_json_ld)
        self.schema_abbr = self.schema_name.split('_')[0]

        # Initialize AttributesExplorer
        self.ae = AttributesExplorer(self.path_to_json_ld)

        # Create output paths.
        self.text_csv_output_path = self.ae.create_output_path('text_csv')
        self.json_output_path = self.ae.create_output_path('tangled_tree_json')

    def strip_double_quotes(self, string):
        # Remove double quotes from beginning and end of string.
        if string.startswith('"') and string.endswith('"'):
            string = string[1:-1]
        # now remove whitespace
        string = "".join(string.split())
        return string

    def get_text_for_tangled_tree(self, text_type, save_file=False):
        '''Gather the text that needs to be either higlighted or plain for the tangled tree visualization.
        Args:
            text_type (str): Choices = ['highlighted', 'plain'], determines the type of text
                rendering to return.
            save_file (bool): Determines if the outputs should be saved to disk or returned.
        Returns:
            If save_file==True: Saves plain or highlighted text as a CSV (to disk).
               save_file==False: Returns plain or highlighted text as a csv string.
        '''
        # Get nodes in the digraph, many more nodes returned if figure type is dependency
        cdg = self.sg.se.get_digraph_by_edge_type(self.dependency_type)
        nodes = cdg.nodes()

        if self.dependency_type == 'requiresComponent':
            component_nodes = nodes
        else:
            # get component nodes if making dependency figure
            component_dg = self.sg.se.get_digraph_by_edge_type('requiresComponent')
            component_nodes = component_dg.nodes()

        # Initialize lists
        highlighted = []
        plain = []

        # For each component node in the tangled tree gather the plain and higlighted text.
        for node in component_nodes:
            # Get the highlighted components based on figure_type
            if self.figure_type == 'component':
                highlight_descendants = self.sg.se.get_descendants_by_edge_type(node, 'requiresComponent')
            elif self.figure_type == 'dependency':
                highlight_descendants = [node]

            
            # Format text to be higlighted and gather text to be formated plain.
            if not highlight_descendants:
                # If there are no highlighted descendants just highlight the selected node (format for observable.)
                highlighted.append([node, "id", node])
                # Gather all the text as plain text.
                plain_descendants = [n for n in nodes if n != node]
            else:
                # Format higlighted text for Observable.
                for hd in highlight_descendants:
                    highlighted.append([node, "id", hd])
                # Gather the non-higlighted text as plain text descendants.
                plain_descendants = [node for node in nodes if node not in highlight_descendants]
            
            # Format all the plain text for observable.
            for nd in plain_descendants:
                plain.append([node, "id", nd])

        # Prepare df depending on what type of text we need.
        df = pd.DataFrame(locals()[text_type.lower()], columns = ['Component', 'type', 'name'])

        # Depending on input either export csv locally to disk or as a string.
        if save_file==True:
            file_name = f"{self.schema_abbr}_{self.figure_type}_{text_type}.csv"
            df.to_csv(os.path.join(self.text_csv_output_path, file_name))
            return
        elif save_file==False:
            return df.to_csv()

    def get_topological_generations(self):
        ''' Gather topological_gen, nodes and edges based on figure type.
        Outputs:
            topological_gen (List(list)):list of lists. Indicates layers of nodes.
            nodes: (Networkx NodeView) Nodes of the component or dependency graph. When iterated over it functions like a list.
            edges: (Networkx EdgeDataView) Edges of component or dependency graph. When iterated over it works like a list of tuples.
        '''
        # Get nodes in the digraph
        digraph = self.sg.se.get_digraph_by_edge_type(self.dependency_type)
        nodes = digraph.nodes()

        # Get subgraph
        mm_graph = self.sg.se.get_nx_schema()
        subg = self.sg.get_subgraph_by_edge_type(mm_graph, self.dependency_type)

        # Get edges and topological_gen based on figure type.
        if self.figure_type == 'component':
            edges = digraph.edges()
            topological_gen = list(reversed(list(nx.topological_generations(subg))))

        elif self.figure_type == 'dependency':
            rev_digraph = nx.DiGraph.reverse(digraph)
            edges = rev_digraph.edges()
            topological_gen = list(nx.topological_generations(subg))
        
        return topological_gen, nodes, edges, subg

    def remove_unwanted_characters_from_conditional_statement(self, cond_req: str) -> str:
        '''Remove unwanted characters from conditional statement
        Example of conditional requirement: If File Format IS "BAM" OR "CRAM" OR "CSV/TSV" then Genome Build is required
        Example output: File Format IS "BAM" OR "CRAM" OR "CSV/TSV"
        '''
        if "then" in cond_req:
            # remove everything after "then"
            cond_req_new = cond_req.split('then')[0]

            # remove "If" and empty space
            cond_req = cond_req_new.replace("If", "").lstrip().rstrip()
        return cond_req

    def get_ca_alias(self, conditional_requirements: list) -> dict:
        '''Get the alias for each conditional attribute. 

        NOTE: Obtaining attributes(attr) and aliases(ali) in this function is specific to how formatting
            is set in AttributesExplorer. If that formatting changes, this section
            will likely break or in the worst case have a silent error.         
        Input:
            conditional_requirements_list (list): list of strings of conditional requirements from outputs of AttributesExplorer.
        Output:
            ca_alias (dict):
                key: alias (attribute response)
                value: attribute
        '''
        ca_alias = {}

        # clean up conditional requirements
        conditional_requirements = [self.remove_unwanted_characters_from_conditional_statement(req) for req in conditional_requirements]

        for i, req in enumerate(conditional_requirements):
            if "OR" not in req:
                attr, ali = req.split(' is ')
                attr = "".join(attr.split())
                ali = self.strip_double_quotes(ali)
                ca_alias[ali] = attr
            else:
                attr, alias_str = req.split(' is ')
                alias_lst = alias_str.split(' OR ')
                for elem in alias_lst:
                    elem = self.strip_double_quotes(elem)
                    ca_alias[elem] = attr
        return ca_alias

    def gather_component_dependency_info(self, cn, attributes_df):
        '''Gather all component dependency information.
        Inputs:
            cn: (str) component name
            attributes_df: (Pandas DataFrame) Details for all attributes across all components. From AttributesExplorer.
        Outputs:
            conditional_attributes (list): List of conditional attributes for a particular component
            ca_alias (dict):
                key: alias (attribute response)
                value: attribute
            all_attributes (list): all attributes associated with a particular component.
        '''

        # Gather all component dependency information
        component_attributes = self.sg.get_descendants_by_edge_type(
                                        cn,
                                        self.dependency_type,
                                        connected=True
                                        )
        
        # Dont want to display `Component` in the figure so remove
        if 'Component' in component_attributes:
            component_attributes.remove('Component')
        
        # Gather conditional attributes so they can be added to the figure.
        if 'Cond_Req' in attributes_df.columns:
            conditional_attributes = list(attributes_df[(attributes_df['Cond_Req']==True)
                &(attributes_df['Component']==cn)]['Label'])
            ca_df = attributes_df[(attributes_df['Cond_Req']==True)&(attributes_df['Component']==cn)]
            conditional_requirements = list(attributes_df[(attributes_df['Cond_Req']==True)
                &(attributes_df['Component']==cn)]['Conditional Requirements'])
            ca_alias = self.get_ca_alias(conditional_requirements)
        else:
            # If there are no conditional attributes/requirements, initialize blank lists.
            conditional_attributes = []
            ca_alias = {}
        
        # Gather a list of all attributes for the current component.
        all_attributes = list(np.append(component_attributes,conditional_attributes))
        
        return conditional_attributes, ca_alias, all_attributes

    def find_source_nodes(self, nodes, edges, all_attributes=[]):
        '''Find all nodes in the graph that do not have a parent node.
        Inputs:
            nodes: (Networkx NodeView) Nodes of the component or dependency graph. When iterated over it functions like a list.
            edges: (Networkx EdgeDataView) Edges of component or dependency graph. When iterated over it works like a list of tuples.
            attributes_df: (Pandas DataFrame) Details for all attributes across all components. From AttributesExplorer.

        Outputs:
            source_nodes (list(str)): List of parentless nodes in 
        '''
        # Find edges that are not source nodes.
        not_source = []
        for node in nodes:
            for edge_pair in edges:
                if node == edge_pair[0]:
                    not_source.append(node)

        # Find source nodes as nodes that are not in not_source.
        source_nodes = []
        for node in nodes:
            if self.figure_type == 'dependency':
                if node not in not_source and node in all_attributes:
                    source_nodes.append(node)
            else:
                if node not in not_source:
                    source_nodes.append(node)
        return source_nodes

    def get_parent_child_dictionary(self, nodes, edges, all_attributes=[]):
        '''Based on the dependency type, create dictionaries between parent and child and child and parent attributes.
        Input:
            nodes: (Networkx NodeView) Nodes of the component or dependency graph.
            edges: (Networkx EdgeDataView (component figure) or List(list) (dependency figure))
                Edges of component or dependency graph.
            all_attributes:
        Output:
            child_parents (dict):
                key: child
                value: list of the childs parents
            parent_children (dict):
                key: parent
                value: list of the parents children
        '''
        child_parents = {}
        parent_children = {}

        if self.dependency_type == 'requiresComponent':
            
            # Construct child_parents dictionary
            for edge in edges:
                
                # Add child as a key
                if edge[0] not in child_parents.keys():
                    child_parents[edge[0]] = []
                
                # Add parents to list
                child_parents[edge[0]].append(edge[1])
            
            # Construct parent_children dictionary
            for edge in edges:
                
                # Add parent as a key
                if edge[1] not in parent_children.keys():
                    parent_children[edge[1]] = []
                
                # Add children to list
                parent_children[edge[1]].append(edge[0])
        
        elif self.dependency_type == 'requiresDependency':
            
            # Construct child_parents dictionary
            for edge in edges:
                
                # Check if child is an attribute for the current component
                if edge[0] in all_attributes:
                    
                    # Add child as a key
                    if edge[0] not in child_parents.keys():
                        child_parents[edge[0]] = []
                    
                    # Add parent to list if it is an attriute for the current component
                    if edge[1] in all_attributes:
                        child_parents[edge[0]].append(edge[1])
            
            # Construct parent_children dictionary
            for edge in edges:
                
                # Check if parent is an attribute for the current component
                if edge[1] in all_attributes:
                    
                    # Add parent as a key
                    if edge[1] not in parent_children.keys():
                        parent_children[edge[1]] = []
                    
                    # Add child to list if it is an attriute for the current component
                    if edge[0] in all_attributes:
                        parent_children[edge[1]].append(edge[0])

        return child_parents, parent_children

    def alias_edges(self, ca_alias:dict, edges) -> List[list]:
        '''Create new edges based on aliasing between an attribute and its response.
        Purpose:
            Create aliased edges.
            For example: 
                If BiospecimenType (attribute) is AnalyteBiospecimenType (response)
                Then ShippingConditionType (conditional requirement) is now required.
            In the model the edges that connect these options are:
                (AnalyteBiospecimenType, BiospecimenType)
                (ShippingConditionType, AnalyteBiospecimenType)
            Use alias defined in self.get_ca_alias along to define new edges that would 
            directly link attributes to their conditional requirements, in this 
            example the new edge would be:
                [ShippingConditionType, BiospecimenType]
        Inputs:
            ca_alias (dict):
                key: alias (attribute response)
                value: attribute
            edges (Networkx EdgeDataView): Edges of component or dependency graph. When iterated over it works like a list of tuples.
        Output:
            aliased_edges (List[lists]) of aliased edges.
        '''
        aliased_edges = []
        for i, edge in enumerate(edges):
            
            # construct one set of edges at a time
            edge_set = []
            
            # If the first edge has an alias add alias to the first position in the current edge set
            if edge[0] in ca_alias.keys():
                edge_set.append(ca_alias[edge[0]])
            
            # Else add the non-aliased edge
            else:
                edge_set.append(edge[0])

            # If the secod edge has an alias add alias to the first position in the current edge set
            if edge[1] in ca_alias.keys():
                edge_set.append(ca_alias[edge[1]])
            
            # Else add the non-aliased edge
            else:
                edge_set.append(edge[1])

            # Add new edge set to a the list of aliased edges.
            aliased_edges.append(edge_set)

        return aliased_edges

    def prune_expand_topological_gen(self, topological_gen, all_attributes, conditional_attributes):
        '''
        Purpose:
            Remake topological_gen with only relevant nodes.
                This is necessary since for the figure this function is being used in we 
                only want to display a portion of the graph data.
            In addition to only displaying relevant nodes, we want to add conditional
                attributes to topological_gen so we can visualize them in the tangled tree
                as well.
        Input:
            topological_gen (List[list]): Indicates layers of nodes.
            all_attributes (list): all attributes associated with a particular component.
            conditional_attributes (list): List of conditional attributes for a particular component
        Output:
            new_top_gen (List[list]): mimics structure of topological_gen but only
                includes the nodes we want
        '''

        pruned_topological_gen = []

        # For each layer(gen) in the topological generation list
        for i, layer in enumerate(topological_gen):
            
            current_layer = []
            next_layer = []
            
            # For each node in the layer
            for node in layer:
                
                # If the node is relevant to this component and is not a conditional attribute add it to the current layer.
                if node in all_attributes and node not in conditional_attributes:
                    current_layer.append(node)
                
                # If its a conditional attribute add it to a followup layer.
                if node in conditional_attributes:
                    next_layer.append(node)

            # Added layers to new pruned_topological_gen list
            if current_layer:
                pruned_topological_gen.append(current_layer)
            if next_layer:
                pruned_topological_gen.append(next_layer)

        return pruned_topological_gen

    def get_base_layers(self, topological_gen, child_parents, source_nodes, cn):
        '''
        Purpose:
            Reconfigure topological gen to move things back appropriate layers if 
            they would have a back reference.

            The Tangle Tree figure requrires an acyclic directed graph that has additional 
                layering rules between connected nodes.
                - If there is a backward connection then the line connecting them will
                    break (this would suggest a cyclic connection.)
                - Additionally if two or more nodes are connecting to a downstream node it is 
                    best to put both parent nodes at the same level, if possible, to 
                    prevent line breaks.
                - Also want to move any children nodes one layer below 
                    the parent node(s). If there are multiple parents, put one layer below the
                    parent that is furthest from the origin.

            This is an iterative process that needs to run twice to move all the nodes to their
            appropriate positions.
        Input:
            topological_gen: list of lists. Indicates layers of nodes.
            child_parents (dict):
                key: child
                value: list of the childs parents
            source_nodes: list, list of nodes that do not have a parent.
            cn: str, component name, default=''
        Output:
            base_layers: dict, key: component name, value: layer
                represents initial layering of toplogical_gen
            base_layers_copy_copy: dict, key: component name, value: layer
                represents the final layering after moving the components/attributes to
                their desired layer.c
        '''
        # Convert topological_gen to a dictionary
        base_layers = {com:i for i, lev in enumerate(topological_gen)
                                for com in lev}
        
        # Make another version to iterate on -- Cant set to equal or will overwrite the original.
        base_layers_copy = {com:i for i, lev in enumerate(topological_gen)
                                for com in lev}

        # Move child nodes one node downstream of their parents.
        for level in topological_gen:
            for node in level:
                
                # Check if node has a parent.
                if node in child_parents.keys():
                    
                    #node_level = base_layers[node]
                    # Look at the parents for the node.
                    parent_levels = []
                    for par in child_parents[node]:
                        
                        # Get the layer the parent is located at.
                        parent_levels.append(base_layers[par])
                        
                        # Get the max layer a parent of the node can be found.
                        max_parent_level = max(parent_levels)

                        # Move the node one layer beyond the max parent node position, so it will be downstream of its parents.
                        base_layers_copy[node] = max_parent_level + 1
        
        # Make another version of updated positions iterate on further.
        base_layers_copy_copy = base_layers_copy

        # Move parental source nodes if necessary.
        for level in topological_gen:
            for node in level:
                
                # Check if node has any parents.
                if node in child_parents.keys():
                    parent_levels = []
                    modify_par = []
                    
                    # For each parent get their position.
                    for par in child_parents[node]:
                        parent_levels.append(base_layers_copy[par])
                    
                    # If one of the parents is a source node move 
                    # it to the same level as the other nodes the child connects to so
                    # that the connections will not be backwards (and result in a broken line)
                    for par in child_parents[node]:
                        
                        # For a given parent determine if its a source node and that the parents 
                        # are not already at level 0, and the parent is not the current component node.
                        if (par in source_nodes and 
                            (parent_levels.count(parent_levels[0]) != len(parent_levels))
                            and par != cn):

                            # If so, remove its position from parent_levels
                            parent_levels.remove(base_layers_copy[par])
                            
                            # Add this parent to a list of parental positions to modify later.
                            modify_par.append(par)
                        
                        # Get the new max parent level for this node.
                        max_parent_level = max(parent_levels)
                        
                        # Move the node one position downstream of its max parent level.
                        base_layers_copy_copy[node] = max_parent_level + 1
                    
                    # For each parental position to modify, move the parents level up to the max_parent_level.
                    for par in modify_par:
                        base_layers_copy_copy[par] = max_parent_level
        
        return base_layers, base_layers_copy_copy

    def adjust_node_placement(self, base_layers_copy_copy, base_layers, topological_gen):
        '''Reorder nodes within topological_generations to match how they were ordered in base_layers_copy_copy
        Input:
            topological_gen: list of lists. Indicates layers of nodes.
            base_layers: dict, key: component name, value: layer
                represents initial layering of toplogical_gen
            base_layers_copy_copy: dict, key: component name, value: layer
                represents the final layering after moving the components/attributes to
                their desired layer.
        Output:
            topological_gen: same format but as the incoming topologial_gen but
                ordered to match base_layers_copy_copy.
        '''
        if self.figure_type == 'component':
            # For each node get its new layer in the tangled tree
            for node, i in base_layers_copy_copy.items():
                
                # Check if node is not already in the proper layer
                if node not in topological_gen[i]:

                    # If not put it in the appropriate layer
                    topological_gen[i].append(node)
                    
                    # Remove from inappropriate layer.
                    topological_gen[base_layers[node]].remove(node)
        
        elif self.figure_type == 'dependency':
            for node, i in base_layers_copy_copy.items():
                
                # Check if the location of the node is more than the number of 
                # layers topological gen current handles
                if i > len(topological_gen) - 1:

                    # If so, add node to new node at the end of topological_gen
                    topological_gen.append([node])
                    
                    # Remove the node from its previous position.
                    topological_gen[base_layers[node]].remove(node)
                
                # Else, check if node is not already in the proper layer
                elif node not in topological_gen[i]:

                    # If not put it in the appropriate layer
                    topological_gen[i].append(node)
                    
                    # Remove from inappropriate layer.
                    topological_gen[base_layers[node]].remove(node)
        return topological_gen

    def move_source_nodes_to_bottom_of_layer(self, node_layers, source_nodes):
        '''For aesthetic purposes move source nodes to the bottom of their respective layers.
        Input:
            node_layers (List(list)): Lists of lists of each layer and the nodes contained in that layer as strings.
            source_nodes (list): list of nodes that do not have a parent.
        Output:
            node_layers (List(list)): modified to move source nodes to the bottom of each layer.
        '''
        for i, layer in enumerate(node_layers):
            nodes_to_move = []
            for node in layer:
                if node in source_nodes:
                    nodes_to_move.append(node)
            for node in nodes_to_move:
                node_layers[i].remove(node)
                node_layers[i].append(node)
        return node_layers

    def get_layers_dict_list(self, node_layers, child_parents, parent_children, all_parent_children):
        '''Convert node_layers to a list of lists of dictionaries that specifies each node and its parents (if applicable).
        Inputs:
            node_layers: list of lists of each layer and the nodes contained in that layer as strings.
            child_parents (dict):
                key: child
                value: list of the childs parents
            parent_children (dict):
                key: parent
                value: list of the parents children
        Outputs:
            layers_list (List(list): list of lists of dictionaries that specifies each node and its parents (if applicable)
        '''
        num_layers = len(node_layers)
        layers_list = [[] for i in range(0, num_layers)]
        for i, layer in enumerate(node_layers):
            for node in layer:
                if node in child_parents.keys():
                    parents = child_parents[node]
                else: 
                    parents = []

                if node in parent_children.keys():
                    direct_children = parent_children[node]
                else: 
                    direct_children = []

                if node in all_parent_children.keys():
                    all_children = all_parent_children[node]
                else: 
                    all_children = []
                layers_list[i].append({'id': node, 'parents': parents, 'direct_children': direct_children, 'children': all_children})

        return layers_list

    def get_node_layers_json(self, topological_gen, source_nodes, child_parents, parent_children, cn='', all_parent_children=None):
        '''Return all the layers of a single tangled tree as a JSON String.
        Inputs:
            topological_gen:list of lists. Indicates layers of nodes.
            source_nodes: list of nodes that do not have a parent.
            child_parents (dict):
                key: child
                value: list of the childs parents
            parent_children (dict):
                key: parent
                value: list of the parents children
            all_parent_children (dict):
                key: parent
                value: list of the parents children (including all downstream nodes). Default to an empty dictionary
        Outputs:
            layers_json (JSON String): Layers of nodes in the tangled tree as a json string.
        '''

        base_layers, base_layers_copy_copy = self.get_base_layers(topological_gen, 
                child_parents, source_nodes, cn)

        # Rearrange node_layers to follow the pattern laid out in component layers.
        node_layers = self.adjust_node_placement(base_layers_copy_copy, 
            base_layers, topological_gen)

        # Move source nodes to the bottom of each layer.
        node_layers = self.move_source_nodes_to_bottom_of_layer(node_layers, source_nodes)

        # Convert layers to a list of dictionaries
        if not all_parent_children:
            # default to an empty dictionary 
            all_parent_children = dict()
            
        layers_dicts = self.get_layers_dict_list(node_layers, child_parents, parent_children, all_parent_children)

        # Convert dictionary to a JSON string
        layers_json = json.dumps(layers_dicts)

        return layers_json

    def save_outputs(self, save_file, layers_json, cn='', all_layers=[]):
        '''
        Inputs:
            save_file (bool): Indicates whether to save a file locally or not.:
            layers_json (JSON String): Layers of nodes in the tangled tree as a json string.
            cn (str): component name, default=''
            all_layers (list of json strings): Each string represents contains the layers for a single tangled tree.
                If a dependency figure the list is added to each time this function is called, so starts incomplete.
                default=[].
        Outputs:
            all_layers (list of json strings): 
                If save_file == False: Each string represents contains the layers for a single tangled tree.
                If save_file ==True: is an empty list.
        '''
        if save_file == True:
            if cn:
                output_file_name = f"{self.schema_abbr}_{self.figure_type}_{cn}_tangled_tree.json"
            else:
                output_file_name = f"{self.schema_abbr}_{self.figure_type}_tangled_tree.json"
            with open(os.path.join(self.json_output_path, output_file_name), 'w') as outfile:
                outfile.write(layers_json)
            logger.info(f"Tangled Tree JSON String saved to {os.path.join(self.json_output_path, output_file_name)}.")
            all_layers = layers_json
        elif save_file == False:
            all_layers.append(layers_json)
        return all_layers

    def get_ancestors_nodes(self, subgraph, components):
        """
        Inputs: 
            subgraph: networkX graph object
            components: a list of nodes 
        outputs: 
            all_parent_children: a dictionary that indicates a list of children (including all the intermediate children) of a given node
        """
        all_parent_children = {}
        for component in components: 
            all_ancestors = self.sg.se.get_nodes_ancestors(subgraph, component)
            all_parent_children[component] = all_ancestors

        return all_parent_children

    def get_tangled_tree_layers(self, save_file=True):
        '''Based on user indicated figure type, construct the layers of nodes of a tangled tree.
        Inputs:
            save_file (bool): Indicates whether to save a file locally or not.
        Outputs:
            all_layers (list of json strings): 
                If save_file == False: Each string represents contains the layers for a single tangled tree.
                If save_file ==True: is an empty list.

        Note on Dependency Tangled Tree:
            If there are many conditional requirements associated with a depependency, and those
            conditional requirements have overlapping attributes associated with them
            the tangled tree will only report one
        
        '''
        # Gather the data model's, topological generations, nodes and edges
        topological_gen, nodes, edges, subg = self.get_topological_generations()

        if self.figure_type == 'component':
            # Gather all source nodes
            source_nodes = self.find_source_nodes(nodes, edges)
            
            # Map all children to their parents and vice versa
            child_parents, parent_children = self.get_parent_child_dictionary(nodes, edges)

            # find all the downstream nodes
            all_parent_children = self.get_ancestors_nodes(subg, parent_children.keys())
            
            # Get the layers that each node belongs to.
            layers_json = self.get_node_layers_json(topological_gen, source_nodes, child_parents, parent_children, all_parent_children=all_parent_children)

            # If indicated save outputs locally else gather all layers.
            all_layers = self.save_outputs(save_file, layers_json)         

        if self.figure_type == 'dependency':
            # Get component digraph and nodes.
            component_dg = self.sg.se.get_digraph_by_edge_type('requiresComponent')
            component_nodes = component_dg.nodes()

            # Get table of attributes.
            attributes_csv_str = self.ae.parse_attributes(save_file=False)
            attributes_df = pd.read_table(StringIO(attributes_csv_str), sep=",")

            
            all_layers =[]
            for cn in component_nodes:
                # Gather attribute and dependency information per node
                conditional_attributes, ca_alias, all_attributes = self.gather_component_dependency_info(cn, attributes_df)

                # Gather all source nodes
                source_nodes = self.find_source_nodes(component_nodes, edges, all_attributes)

                # Alias the conditional requirement edge back to its actual parent label,
                # then apply aliasing back to the edges
                aliased_edges = self.alias_edges(ca_alias, edges)

                # Gather relationships between children and their parents.
                child_parents, parent_children = self.get_parent_child_dictionary(nodes,
                    aliased_edges, all_attributes)

                # Remake topological_gen so it has only relevant nodes.
                pruned_topological_gen = self.prune_expand_topological_gen(topological_gen, all_attributes, conditional_attributes)

                # Get the layers that each node belongs to.
                layers_json = self.get_node_layers_json(pruned_topological_gen, source_nodes, child_parents, parent_children, cn)

                # If indicated save outputs locally else, gather all layers.
                all_layers = self.save_outputs(save_file, layers_json, cn, all_layers)
        return all_layers

   