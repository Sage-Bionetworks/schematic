import os
from schematic.schemas.generator import SchemaGenerator

from definitions import DATA_PATH, CONFIG_PATH
from schematic.utils.config_utils import load_yaml

config_data = load_yaml(CONFIG_PATH)

PATH_TO_JSONLD = os.path.join(DATA_PATH, config_data["model"]["input"]["location"])

# create an object of SchemaGenerator() class
schema_generator = SchemaGenerator(PATH_TO_JSONLD)

if isinstance(schema_generator, SchemaGenerator):
    print("'schema_generator' - an object of the SchemaGenerator class has been created successfully.")
else:
    print("object of class SchemaGenerator could not be created.")

# get list of the out-edges from a node based on a specific relationship
TEST_NODE = "Sequencing"
TEST_REL = "parentOf"

out_edges = schema_generator.get_edges_by_relationship(TEST_NODE, TEST_REL)

if out_edges:
    print("The out-edges from class {}, based on {} relationship are: {}".format(TEST_NODE, TEST_REL, out_edges))
else:
    print("The class does not have any out-edges.")

# get list of nodes that are adjacent to specified node, based on a given relationship
adj_nodes = schema_generator.get_adjacent_nodes_by_relationship(TEST_NODE, TEST_REL)

if adj_nodes:
    print("The node(s) adjacent to {}, based on {} relationship are: {}".format(TEST_NODE, TEST_REL, adj_nodes))
else:
    print("The class does not have any adjacent nodes.")

# get list of descendants (nodes) based on a specific type of relationship
desc_nodes = schema_generator.get_descendants_by_edge_type(TEST_NODE, TEST_REL)

if desc_nodes:
    print("The descendant(s) from {} are: {}".format(TEST_NODE, desc_nodes))
else:
    print("The class does not have descendants.")

# get all components associated with a given component
TEST_COMP = "Patient"
req_comps = schema_generator.get_component_requirements(TEST_COMP)

if req_comps:
    print("The component(s) that are associated with a given component: {}".format(req_comps))
else:
    print("There are no components associated with {}".format(TEST_COMP))

# get immediate dependencies that are related to a given node
node_deps = schema_generator.get_node_dependencies(TEST_COMP)

if node_deps:
    print("The immediate dependencies of {} are: {}".format(TEST_COMP, node_deps))
else:
    print("The node has no immediate dependencies.")

# get label for a given node
try:
    node_label = schema_generator.get_node_label(TEST_NODE)

    print("The label name for the node {} is: {}".format(TEST_NODE, node_label))
except KeyError:
    print("Please try a valid node name.")

# get node definition/comment
try:
    node_def = schema_generator.get_node_definition(TEST_NODE)

    print("The node definition for node {} is: {}".format(TEST_NODE, node_def))
except KeyError:
    print("Please try a valid node name.")

# gather dependencies and value-constraints for a particular node
json_schema = schema_generator.get_json_schema_requirements(TEST_COMP, "Patient-Schema")

print("The JSON schema based on {} as source node is:".format(TEST_COMP))
print(json_schema)