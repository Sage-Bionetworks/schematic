"""DataModel Graph"""

import logging
from typing import Any, Optional, Union

import graphviz  # type: ignore
import networkx as nx  # type: ignore
from opentelemetry import trace

from schematic.schemas.data_model_edges import DataModelEdges
from schematic.schemas.data_model_nodes import DataModelNodes
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.utils.general import unlist
from schematic.utils.schema_utils import (
    DisplayLabelType,
    extract_component_validation_rules,
    get_class_label_from_display_name,
    get_property_label_from_display_name,
)
from schematic.utils.validate_utils import rule_in_rule_list
from schematic.utils.viz_utils import visualize

logger = logging.getLogger(__name__)


logger = logging.getLogger(__name__)
tracer = trace.get_tracer("Schematic")


class DataModelGraphMeta:  # pylint: disable=too-few-public-methods
    """DataModelGraphMeta"""

    _instances: dict = {}

    def __call__(  # pylint: disable=no-self-argument
        cls, *args: Any, **kwargs: Any
    ) -> Any:
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)  # type: ignore # pylint: disable=no-member
            cls._instances[cls] = instance
        return cls._instances[cls]


class DataModelGraph:  # pylint: disable=too-few-public-methods
    """
    Generate graph network (networkx) from the attributes and relationships returned
    from the data model parser.

    Create a singleton.
    """

    __metaclass__ = DataModelGraphMeta

    def __init__(
        self,
        attribute_relationships_dict: dict,
        data_model_labels: DisplayLabelType = "class_label",
    ) -> None:
        """Load parsed data model.
        Args:
            attributes_relationship_dict, dict: generated in data_model_parser
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
            data_model_labels: str, display_label or class_label.
                display_label, use the display name as a label, if it is valid
                (contains no blacklisted characters) otherwise will default to schema_label.
                class_label, default, use standard class or property label.
        Raises:
            ValueError, attribute_relationship_dict not loaded.
        """
        self.attribute_relationships_dict = attribute_relationships_dict
        self.dmn = DataModelNodes(self.attribute_relationships_dict)
        self.dme = DataModelEdges()
        self.dmr = DataModelRelationships()
        self.data_model_labels = data_model_labels

        if not self.attribute_relationships_dict:
            raise ValueError(
                (
                    "Something has gone wrong, a data model was not loaded into the DataModelGraph "
                    "Class. Please check that your paths are correct"
                )
            )
        self.graph = self.generate_data_model_graph()

    @tracer.start_as_current_span("DataModelGraph::generate_data_model_graph")
    def generate_data_model_graph(self) -> nx.MultiDiGraph:
        """
        Generate NetworkX Graph from the Relationships/attributes dictionary, the graph is built
          by first adding all nodes to the graph, then connecting nodes by the relationships defined
          in the attributes_relationship dictionary.
        Returns:
            G: nx.MultiDiGraph, networkx graph representation of the data model
        """
        # Get all relationships with edges
        edge_relationships = self.dmr.retreive_rel_headers_dict(edge=True)

        # Find all nodes
        all_nodes = self.dmn.gather_all_nodes_in_model(
            attr_rel_dict=self.attribute_relationships_dict
        )

        # Instantiate NetworkX MultiDigraph
        graph = nx.MultiDiGraph()

        all_node_dict = {}

        ## Fill in MultiDigraph with nodes
        for node in all_nodes:
            # Gather information for each node
            node_dict = self.dmn.generate_node_dict(
                node_display_name=node,
                attr_rel_dict=self.attribute_relationships_dict,
                data_model_labels=self.data_model_labels,
            )

            # Add each node to the all_node_dict to be used for generating edges
            all_node_dict[node] = node_dict

            # Generate node and attach information (attributes) to each node
            graph = self.dmn.generate_node(graph, node_dict)

        edge_list: list[tuple[str, str, dict[str, Union[str, int]]]] = []
        ## Connect nodes via edges
        for node in all_nodes:
            # Generate edges
            edge_list_2 = self.dme.generate_edge(
                node,
                all_node_dict,
                self.attribute_relationships_dict,
                edge_relationships,
                edge_list,
            )
            edge_list = edge_list_2.copy()

        # Add edges to the Graph
        for node_1, node_2, edge_dict in edge_list:
            graph.add_edge(
                node_1, node_2, key=edge_dict["key"], weight=edge_dict["weight"]
            )
        return graph


class DataModelGraphExplorer:  # pylint: disable=too-many-public-methods
    """DataModelGraphExplorer"""

    def __init__(
        self,
        graph: nx.MultiDiGraph,
    ):
        """Load data model graph as a singleton.
        Args:
            G: nx.MultiDiGraph, networkx graph representation of the data model
        """
        self.graph = graph  # At this point the graph is expected to be fully formed.
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary

    def find_properties(self) -> set[str]:
        """
        Identify all properties, as defined by the first node in a pair, connected with
        'domainIncludes' edge type

        Returns:
            properties, set: All properties defined in the data model, each property name
              is defined by its label.
        """
        properties_list: list[str] = []
        for node_1, _, rel in self.graph.edges:
            if rel == self.rel_dict["domainIncludes"]["edge_key"]:
                properties_list.append(node_1)
        properties_set = set(properties_list)
        return properties_set

    def find_classes(self) -> set[str]:
        """
        Identify all classes, as defined but all nodes, minus all properties
        (which are explicitly defined)
        Returns:
            classes, set:  All classes defined in the data model, each class
              name is defined by its label.
        """
        nodes = self.graph.nodes
        properties = self.find_properties()
        classes = nodes - properties
        return classes

    def find_node_range(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> list:
        """Get valid values for the given node (attribute)
        Args:
            node_label, str, Optional[str]: label of the node for which to retrieve valid values
            node_display_name, str, Optional[str]: Display Name of the node for which to
              retrieve valid values
        Returns:
            valid_values, list: List of valid values associated with the provided node.
        """
        if not node_label:
            assert node_display_name is not None
            node_label = self.get_node_label(node_display_name)

        valid_values = []
        for node_1, node_2, rel in self.graph.edges:
            if (
                node_1 == node_label
                and rel == self.rel_dict["rangeIncludes"]["edge_key"]
            ):
                valid_values.append(node_2)
        valid_values = list(set(valid_values))
        return valid_values

    def get_adjacent_nodes_by_relationship(
        self, node_label: str, relationship: str
    ) -> list[str]:
        """Get a list of nodes that is / are adjacent to a given node, based on a relationship type.

        Args:
            node_label: label of the the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.

        Returns:
            List of nodes that are adjacent to the given node.
        #checked
        """
        nodes = set()
        for _, node_2, key, _ in self.graph.out_edges(node_label, data=True, keys=True):
            if key == relationship:
                nodes.add(node_2)

        return list(nodes)

    def get_component_node_required(
        self,
        manifest_component: str,
        node_validation_rules: Optional[list[str]] = None,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
    ) -> bool:
        """Check if a node is required taking into account the manifest component it is defined in
        (requirements can be set in validaiton rule as well as required column)
        Args:
            manifest_component: str, manifest component display name that the node belongs to.
            node_validation_rules: list[str], valdation rules for a given node and component.
            node_label: str, Label of the node you would want to get the comment for.
            node_display_name: str, node display name for the node being queried.
        Returns:
            True, if node is required, False if not
        """
        node_required = False

        if not node_validation_rules:
            # Get node validation rules for a given component
            node_validation_rules = self.get_component_node_validation_rules(
                manifest_component=manifest_component,
                node_label=node_label,
                node_display_name=node_display_name,
            )

        # Check if the valdation rule specifies that the node is required for this particular
        # component.
        if rule_in_rule_list("required", node_validation_rules):
            node_required = True
            # To prevent any unintended errors, ensure the Required field for this node is False
            if self.get_node_required(
                node_label=node_label, node_display_name=node_display_name
            ):
                if not node_display_name:
                    assert node_label is not None
                    node_display_name = self.graph.nodes[node_label][
                        self.rel_dict["displayName"]["node_label"]
                    ]
                error_str = " ".join(
                    [
                        f"For component: {manifest_component} and attribute: {node_display_name}",
                        "requirements are being specified in both the Required field and in the",
                        "Validation Rules. If you desire to use validation rules to set component",
                        "specific requirements for this attribute",
                        "then the Required field needs to be set to False, or the validation may",
                        "not work as intended, for other components where the attribute",
                        "that should not be required.",
                    ]
                )

                logger.error(error_str)
        else:
            # If requirements are not being set in the validaiton rule, then just pull the
            # standard node requirements from the model
            node_required = self.get_node_required(
                node_label=node_label, node_display_name=node_display_name
            )
        return node_required

    def get_component_node_validation_rules(
        self,
        manifest_component: str,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
    ) -> list:
        """Get valdation rules for a given node and component.
        Args:
            manifest_component: str, manifest component display name that the node belongs to.
            node_label: str, Label of the node you would want to get the comment for.
            node_display_name: str, node display name for the node being queried.
        Returns:
            validation_rules: list, validation rules list for a given node and component.
        """
        # get any additional validation rules associated with this node (e.g. can this node
        # be mapped to a list of other nodes)
        node_validation_rules = self.get_node_validation_rules(
            node_label=node_label, node_display_name=node_display_name
        )

        # Parse the validation rules per component if applicable
        if node_validation_rules and isinstance(node_validation_rules, dict):
            node_validation_rules_list = extract_component_validation_rules(
                manifest_component=manifest_component,
                validation_rules_dict=node_validation_rules,  # type: ignore
            )
        else:
            assert isinstance(node_validation_rules, list)
            node_validation_rules_list = node_validation_rules
        return node_validation_rules_list

    def get_component_requirements(
        self,
        source_component: str,
    ) -> list[str]:
        """
        Get all components that are associated with a given source component and are
          required by it.

        Args:
            source_component: source component for which we need to find all required downstream
              components.

        Returns:
            List of nodes that are descendants from the source component are are related to the
              source through a specific component relationship.
        """

        req_components = list(
            reversed(
                self.get_descendants_by_edge_type(
                    source_component,
                    self.rel_dict["requiresComponent"]["edge_key"],
                    ordered=True,
                )
            )
        )

        return req_components

    def get_component_requirements_graph(
        self,
        source_component: str,
    ) -> nx.DiGraph:
        """
        Get all components that are associated with a given source component and are required by it;
          return the components as a dependency graph (i.e. a DAG).

        Args:
            source_component, str: source component for which we need to find all required
              downstream components.

        Returns:
            A subgraph of the schema graph induced on nodes that are descendants from the source
              component and are related to the source through a specific component relationship.
        """

        # get a list of required component nodes
        req_components = self.get_component_requirements(source_component)

        # get the subgraph induced on required component nodes
        req_components_graph = self.get_subgraph_by_edge_type(
            self.rel_dict["requiresComponent"]["edge_key"],
        ).subgraph(req_components)

        return req_components_graph

    def get_descendants_by_edge_type(
        self,
        source_node: str,
        relationship: str,
        connected: bool = True,
        ordered: bool = False,
    ) -> list[str]:
        """
        Get all nodes that are descendants of a given source node, based on a specific
          type of edge / relationship type.

        Args:
            source_node: The node whose descendants need to be retreived.
            relationship: Edge / link relationship type with possible values same as in above docs.
            connected:
              If True, we need to ensure that all descendant nodes are reachable from the source
                node, i.e., they are part of the same connected component.
              If False, the descendants could be in multiple connected components.
              Default value is True.
            ordered:
              If True, the list of descendants will be topologically ordered.
              If False, the list has no particular order (depends on the order in which the
                descendats were traversed in the subgraph).

        Returns:
            List of nodes that are descendants from a particular node (sorted / unsorted)
        """

        root_descendants = nx.descendants(self.graph, source_node)

        subgraph_nodes = list(root_descendants)
        subgraph_nodes.append(source_node)
        descendants_subgraph = self.graph.subgraph(subgraph_nodes)

        # prune the descendants subgraph so as to include only those edges that match
        # the relationship type
        rel_edges = []
        for node_1, node_2, key, _ in descendants_subgraph.edges(data=True, keys=True):
            if key == relationship:
                rel_edges.append((node_1, node_2))

        relationship_subgraph = nx.DiGraph()
        relationship_subgraph.add_edges_from(rel_edges)

        descendants = relationship_subgraph.nodes()

        if not descendants:
            # return empty list if there are no nodes that are reachable from the
            # source node based on this relationship type
            return []

        if connected and ordered:
            # get the set of reachable nodes from the source node
            descendants = nx.descendants(relationship_subgraph, source_node)
            descendants.add(source_node)

            # normally, the descendants from a node are unordered (peculiarity
            # of nx descendants call)
            # form the subgraph on descendants and order it topologically
            # this assumes an acyclic subgraph
            descendants = nx.topological_sort(
                relationship_subgraph.subgraph(descendants)
            )
        elif connected:
            # get the nodes that are reachable from a given source node
            # after the pruning process above some nodes in the
            # root_descendants subgraph might have become disconnected and
            # will be omitted
            descendants = nx.descendants(relationship_subgraph, source_node)
            descendants.add(source_node)
        elif ordered:
            # sort the nodes topologically
            # this requires the graph to be an acyclic graph
            descendants = nx.topological_sort(relationship_subgraph)

        return list(descendants)

    def get_digraph_by_edge_type(self, edge_type: str) -> nx.DiGraph:
        """Get a networkx digraph of the nodes connected via a given edge_type.
        Args:
            edge_type:
                Edge type to search for, possible types are defined by 'edge_key'
                  in relationship class
        Returns:
        """
        digraph = nx.DiGraph()
        for node_1, node_2, key, _ in self.graph.edges(data=True, keys=True):
            if key == edge_type:
                digraph.add_edge(node_1, node_2)
        return digraph

    def get_edges_by_relationship(
        self,
        node: str,
        relationship: str,
    ) -> list[tuple[str, str]]:
        """Get a list of out-edges of a node where the edges match a specifc type of relationship.

        i.e., the edges connecting a node to its neighbors are of relationship type -- "parentOf"
          (set of edges to children / sub-class nodes).

        Args:
            node: the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.

        Returns:
            List of edges that are connected to the node.
        """
        edges: list[tuple[str, str]] = []

        for node_1, node_2, key, _ in self.graph.out_edges(node, data=True, keys=True):
            if key == relationship:
                edges.append((node_1, node_2))

        return edges

    def get_ordered_entry(self, key: str, source_node_label: str) -> list[str]:
        """
        Order the values associated with a particular node and edge_key to
          match original ordering in schema.

        Args:
            key (str): a key representing and edge relationship in
              DataModelRelationships.relationships_dictionary
            source_node_label (str): node to look for edges of and order

        Raises:
            KeyError: cannot find source node in graph

        Returns:
            list[str]:
              list of sorted nodes, that share the specified relationship with the source node
              For the example data model, for key='rangeIncludes', source_node_label='CancerType'
                the return would be ['Breast, 'Colorectal', 'Lung', 'Prostate', 'Skin'] in that
                exact order.
        """
        # Check if node is in the graph, if not throw an error.
        if not self.is_class_in_schema(node_label=source_node_label):
            raise KeyError(
                f"Cannot find node: {source_node_label} in the graph, please check entry."
            )

        edge_key = self.rel_dict[key]["edge_key"]

        # Handle out edges
        if self.rel_dict[key]["jsonld_direction"] == "out":
            # use outedges

            original_edge_weights_dict = {
                attached_node: self.graph[source_node][attached_node][edge_key][
                    "weight"
                ]
                for source_node, attached_node in self.graph.out_edges(
                    source_node_label
                )
                if edge_key in self.graph[source_node][attached_node]
            }
        # Handle in edges
        else:
            # use inedges
            original_edge_weights_dict = {
                attached_node: self.graph[attached_node][source_node][edge_key][
                    "weight"
                ]
                for attached_node, source_node in self.graph.in_edges(source_node_label)
                if edge_key in self.graph[attached_node][source_node]
            }

        sorted_nodes = list(
            dict(
                sorted(original_edge_weights_dict.items(), key=lambda item: item[1])
            ).keys()
        )

        return sorted_nodes

    # Get values associated with a node
    def get_nodes_ancestors(self, subgraph: nx.DiGraph, node_label: str) -> list[str]:
        """Get a list of nodes reachable from source component in graph

        Args:
            subgraph (nx.DiGraph): networkx graph object
            node_label (str): label of node to find ancestors for

        Returns:
            list[str]: nodes reachable from source in graph
        """
        all_ancestors = list(nx.ancestors(subgraph, node_label))

        return all_ancestors

    def get_node_comment(
        self, node_display_name: Optional[str] = None, node_label: Optional[str] = None
    ) -> str:
        """Get the node definition, i.e., the "comment" associated with a given node display name.

        Args:
            node_display_name, str: Display name of the node which you want to get the comment for.
            node_label, str: Label of the node you would want to get the comment for.
        Returns:
            Comment associated with node, as a string.
        """
        if not node_label:
            assert node_display_name is not None
            node_label = self.get_node_label(node_display_name)

        if not node_label:
            return ""

        node_definition = self.graph.nodes[node_label][
            self.rel_dict["comment"]["node_label"]
        ]
        return node_definition

    def get_node_dependencies(
        self,
        source_node: str,
        display_names: bool = True,
        schema_ordered: bool = True,
    ) -> list[str]:
        """Get the immediate dependencies that are related to a given source node.

        Args:
            source_node: The node whose dependencies we need to compute.
            display_names: if True, return list of display names of each of the dependencies.
                           if False, return list of node labels of each of the dependencies.
            schema_ordered:
              if True, return the dependencies of the node following the order of the schema
                (slower).
              if False, return dependencies from graph without guaranteeing schema order (faster)

        Returns:
            List of nodes that are dependent on the source node.
        """

        if schema_ordered:
            # get dependencies in the same order in which they are defined in the schema
            required_dependencies = self.get_ordered_entry(
                key=self.rel_dict["requiresDependency"]["edge_key"],
                source_node_label=source_node,
            )
        else:
            required_dependencies = self.get_adjacent_nodes_by_relationship(
                node_label=source_node,
                relationship=self.rel_dict["requiresDependency"]["edge_key"],
            )

        if display_names:
            # get display names of dependencies
            dependencies_display_names = []

            for req in required_dependencies:
                dependencies_display_names.append(
                    self.graph.nodes[req][self.rel_dict["displayName"]["node_label"]]
                )

            return dependencies_display_names

        return required_dependencies

    def get_nodes_descendants(self, node_label: str) -> list[str]:
        """Return a list of nodes reachable from source in graph
        Args:
            node_label, str: any given node
        Return:
            all_descendants, list: nodes reachable from source in graph
        """
        all_descendants = list(nx.descendants(self.graph, node_label))

        return all_descendants

    def get_nodes_display_names(
        self,
        node_list: list[str],
    ) -> list[str]:
        """Get display names associated with the given list of nodes.

        Args:
            node_list: List of nodes whose display names we need to retrieve.

        Returns:
            List of display names.
        """
        node_list_display_names = [
            self.graph.nodes[node][self.rel_dict["displayName"]["node_label"]]
            for node in node_list
        ]

        return node_list_display_names

    def get_node_label(self, node_display_name: str) -> str:
        """Get the node label for a given display name.

        Args:
            node_display_name: Display name of the node which you want to get the label for.
        Returns:
            Node label associated with given node.
            If display name not part of schema, return an empty string.
        """

        node_class_label = get_class_label_from_display_name(
            display_name=node_display_name
        )
        node_property_label = get_property_label_from_display_name(
            display_name=node_display_name
        )

        if node_class_label in self.graph.nodes:
            node_label = node_class_label
        elif node_property_label in self.graph.nodes:
            node_label = node_property_label
        else:
            node_label = ""

        return node_label

    def get_node_range(
        self,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
        display_names: bool = False,
    ) -> list[str]:
        """
        Get the range, i.e., all the valid values that are associated with a node label.


        Args:
            node_label (Optional[str], optional): Node for which you need to retrieve the range.
              Defaults to None.
            node_display_name (Optional[str], optional): _description_. Defaults to None.
            display_names (bool, optional): _description_. Defaults to False.

        Raises:
            ValueError: If the node cannot be found in the graph.

        Returns:
            list[str]:
              If display_names=False, a list of valid values (labels) associated with a given node.
              If display_names=True, a list of valid values (display names) associated
                with a given node
        """
        if not node_label:
            assert node_display_name is not None
            node_label = self.get_node_label(node_display_name)

        try:
            # get node range in the order defined in schema for given node
            required_range = self.find_node_range(node_label=node_label)
        except KeyError as exc:
            raise ValueError(
                f"The source node {node_label} does not exist in the graph. "
                "Please use a different node."
            ) from exc

        if display_names:
            # get the display name(s) of all dependencies
            dependencies_display_names = []

            for req in required_range:
                dependencies_display_names.append(self.graph.nodes[req]["displayName"])

            return dependencies_display_names

        return required_range

    def get_node_required(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> bool:
        """Check if a given node is required or not.

        Note: The possible options that a node can be associated with -- "required" / "optional".

        Args:
            node_label: Label of the node for which you need to look up.
            node_display_name: Display name of the node for which you want look up.
        Returns:
            True: If the given node is a "required" node.
            False: If the given node is not a "required" (i.e., an "optional") node.
        """
        if not node_label:
            assert node_display_name is not None
            node_label = self.get_node_label(node_display_name)

        rel_node_label = self.rel_dict["required"]["node_label"]
        node_required = self.graph.nodes[node_label][rel_node_label]
        return node_required

    def get_node_validation_rules(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> Union[list, dict[str, str]]:
        """Get validation rules associated with a node,

        Args:
            node_label: Label of the node for which you need to look up.
            node_display_name: Display name of the node which you want to get the label for.
        Returns:
            A set of validation rules associated with node, as a list or a dictionary.
        """
        if not node_label:
            if node_display_name is None:
                raise ValueError(
                    "Either node_label or node_display_name must be provided."
                )

            # try search node label using display name
            node_label = self.get_node_label(node_display_name)

        if not node_label:
            return []

        try:
            node_validation_rules = self.graph.nodes[node_label]["validationRules"]
        except KeyError as key_error:
            raise ValueError(
                f"{node_label} is not in the graph, please provide a proper node label"
            ) from key_error

        return node_validation_rules

    def get_subgraph_by_edge_type(self, relationship: str) -> nx.DiGraph:
        """Get a subgraph containing all edges of a given type (aka relationship).

        Args:
            relationship: edge / link relationship type with possible values same as in above docs.

        Returns:
            Directed graph on edges of a particular type (aka relationship)
        """

        # prune the metadata model graph so as to include only those edges that
        # match the relationship type
        rel_edges = []
        for node_1, node_2, key, _ in self.graph.out_edges(data=True, keys=True):
            if key == relationship:
                rel_edges.append((node_1, node_2))

        relationship_subgraph = nx.DiGraph()
        relationship_subgraph.add_edges_from(rel_edges)

        return relationship_subgraph

    def find_adjacent_child_classes(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> list[str]:
        """Find child classes of a given node.
        Args:
            node_display_name: Display name of the node to look up.
            node_label: Label of the node to look up.
        Returns:
            List of nodes that are adjacent to the given node, by SubclassOf relationship.
        """
        if not node_label:
            assert node_display_name is not None
            node_label = self.get_node_label(node_display_name)

        return self.get_adjacent_nodes_by_relationship(
            node_label=node_label, relationship=self.rel_dict["subClassOf"]["edge_key"]
        )

    def find_child_classes(self, schema_class: str) -> list:
        """Find schema classes that inherit from the given class
        Args:
            schema_class: node label for the class to from which to look for children.
        Returns:
            list of children to the schema_class.
        """
        child_classes = unlist(list(self.graph.successors(schema_class)))
        assert isinstance(child_classes, list)
        return child_classes

    def find_class_specific_properties(self, schema_class: str) -> list[str]:
        """Find properties specifically associated with a given class
        Args:
            schema_class, str: node/class label, to identify properties for.
        Returns:
            properties, list: List of properties associate with a given schema class.
        Raises:
            KeyError: Key error is raised if the provded schema_class is not in the graph
        """

        if not self.is_class_in_schema(schema_class):
            raise KeyError(
                (
                    f"Schema_class provided: {schema_class} is not in the data model, please check "
                    "that you are providing the proper class/node label"
                )
            )

        properties = []
        for node1, node2 in self.graph.edges():
            if (
                node2 == schema_class
                and "domainValue" in self.graph[node1][schema_class]
            ):
                properties.append(node1)
        return properties

    def find_parent_classes(self, node_label: str) -> list[list[str]]:
        """Find all parents of the provided node
        Args:
            node_label: label of the node to find parents of
        Returns:
            List of list of Parents to the given node.
        """
        # Get digraph of nodes with parents
        digraph = self.get_digraph_by_edge_type("parentOf")

        # Get root node
        root_node = list(nx.topological_sort(digraph))[0]

        # Get paths between root_node and the target node.
        paths = nx.all_simple_paths(self.graph, source=root_node, target=node_label)

        return [_path[:-1] for _path in paths]

    def full_schema_graph(self, size: Optional[int] = None) -> graphviz.Digraph:
        """Create a graph of the data model.
        Args:
            size, float: max height and width of the graph, if one value provided
               it is used for both.
        Returns:
            schema graph viz
        """
        edges = self.graph.edges()
        return visualize(edges, size=size)

    def is_class_in_schema(self, node_label: str) -> bool:
        """Determine if provided node_label is in the schema graph/data model.
        Args:
            node_label: label of node to search for in the
        Returns:
            True, if node is in the graph schema
            False, if node is not in graph schema
        """
        return node_label in self.graph.nodes()

    def sub_schema_graph(
        self, source: str, direction: str, size: Optional[float] = None
    ) -> Optional[graphviz.Digraph]:
        """Create a sub-schema graph
        Args:
            source, str: source node label to start graph
            direction, str: direction to create the vizualization, choose from "up", "down", "both"
            size, float: max height and width of the graph, if one value provided it is used for
              both.
        Returns:
            Sub-schema graph viz
        """
        if direction == "down":
            edges = list(nx.edge_bfs(self.graph, [source]))
            return visualize(edges, size=size)
        if direction == "up":
            paths = self.find_parent_classes(source)
            edges = []
            for _path in paths:
                _path.append(source)
                for i in range(0, len(_path) - 1):
                    edges.append((_path[i], _path[i + 1]))
            return visualize(edges, size=size)
        if direction == "both":
            paths = self.find_parent_classes(source)
            edges = list(nx.edge_bfs(self.graph, [source]))
            for _path in paths:
                _path.append(source)
                for i in range(0, len(_path) - 1):
                    edges.append((_path[i], _path[i + 1]))
            return visualize(edges, size=size)
        return None
