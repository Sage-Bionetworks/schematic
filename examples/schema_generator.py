#!/usr/bin/env python3

import os
import argparse

from schematic.schemas.generator import SchemaGenerator
from schematic import CONFIG

# Create command-line argument parser
parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument("schema_class", metavar="SCHEMA CLASS", help="Name of class from schema.")
parser.add_argument("relationship", metavar="RELATIONSHIP NAME", help="Name of relationship from schema.")
parser.add_argument("data_type", metavar="DATA TYPE NAME", help="Name of data type from schema.")
parser.add_argument("--schema_name", metavar="SCHEMA NAME", help="Name of schema generated based on specified data_type.")
parser.add_argument("--config", "-c", help="Configuration YAML file.")
args = parser.parse_args()

# Load configuration
config_data = CONFIG.load_config(args.config)

PATH_TO_JSONLD = CONFIG["model"]["input"]["location"]

# create an object of SchemaGenerator() class
schema_generator = SchemaGenerator(PATH_TO_JSONLD)

if isinstance(schema_generator, SchemaGenerator):
    print("'schema_generator' - an object of the SchemaGenerator class has been created successfully.")
else:
    print("object of class SchemaGenerator could not be created.")

# get list of the out-edges from a node based on a specific relationship
TEST_NODE = args.schema_class
TEST_REL = args.relationship

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

# get all data_types associated with a given data_type
TEST_DATA_TYPE = args.data_type
req_comps = schema_generator.get_component_requirements(TEST_DATA_TYPE)

if req_comps:
    print("The data type(s) that are associated with a given data type: {}".format(req_comps))
else:
    print("There are no data_types associated with {}".format(TEST_DATA_TYPE))

# get immediate dependencies that are related to a given node
node_deps = schema_generator.get_node_dependencies(TEST_DATA_TYPE)

if node_deps:
    print("The immediate dependencies of {} are: {}".format(TEST_DATA_TYPE, node_deps))
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
if args.schema_name:
    json_schema = schema_generator.get_json_schema_requirements(TEST_DATA_TYPE, args.schema_name + "-Schema")
else:
    json_schema = schema_generator.get_json_schema_requirements(TEST_DATA_TYPE, TEST_DATA_TYPE + "-Schema")


print("The JSON schema based on {} as source node is:".format(TEST_DATA_TYPE))
print(json_schema)