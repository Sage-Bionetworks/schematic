from schematic.schemas.explorer import SchemaExplorer
import networkx as nx
import os
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
from graphviz import Digraph, Source

from definitions import DATA_PATH, CONFIG_PATH
from schematic.utils.config_utils import load_yaml

config_data = load_yaml(CONFIG_PATH)

PATH_TO_JSONLD = os.path.join(DATA_PATH, config_data["model"]["input"]["location"])

# create an object of the SchemaExplorer() class
schema_explorer = SchemaExplorer()

# if isinstance(schema_explorer, SchemaExplorer):
#     print("'schema_explorer' - an object of the SchemaExplorer class has been created successfully.")
# else:
#     print("object of class SchemaExplorer could not be created.")

# # by default schema exploerer loads the biothings schema
# # to explicitly load a different data model/json-ld schema use load_schema()
# schema_explorer.load_schema(PATH_TO_JSONLD)
# print("schema at {} has been loaded.".format(PATH_TO_JSONLD))

# # get the networkx graph generated from the json-ld
# nx_graph = schema_explorer.get_nx_schema()

# if isinstance(nx_graph, nx.MultiDiGraph):
#     print("'nx_graph' - object of class MultiDiGraph has been retreived successfully.")
# else:
#     print("object of class SchemaExplorer could not be retreived.")

# # check if a particular class is in the current HTAN JSON-LD schema (or any schema that has been loaded)
# TEST_CLASS = 'Sequencing'
# is_or_not = schema_explorer.is_class_in_schema(TEST_CLASS)

# if is_or_not == True:
#     print("The class {} is present in the schema.".format(TEST_CLASS))
# else:
#     print("The class {} is not present in the schema.".format(TEST_CLASS))

# graph visualization of the entire HTAN JSON-LD schema
gv_digraph = schema_explorer.full_schema_graph()

# since the graph is very big, we will generate an svg viz. of it
gv_digraph.format = 'svg'
gv_digraph.render(os.path.join(DATA_PATH, '', 'viz/example-GV'), view=True)
print("The svg visualization of the entire schema has been rendered.")

# # graph visualization of a sub-schema
# seq_subgraph = schema_explorer.sub_schema_graph(TEST_CLASS, "up")

# seq_subgraph.format = 'svg'
# seq_subgraph.render('SUB-GV', view=True)
# print("The svg visualization of the sub-schema with {} as the source node has been rendered.".format(TEST_CLASS))

# # returns list of successors of a node
# seq_children = schema_explorer.find_children_classes(TEST_CLASS)
# print("These are the children of {} class: {}".format(TEST_CLASS, seq_children))

# # returns list of parents of a node
# seq_parents = schema_explorer.find_parent_classes(TEST_CLASS)
# print("These are the parents of {} class: {}".format(TEST_CLASS, seq_parents))

# # find the properties that are associated with a class
# PROP_CLASS = 'BiologicalEntity'
# class_props = schema_explorer.find_class_specific_properties(PROP_CLASS)
# print("The properties associated with class {} are: {}".format(PROP_CLASS, class_props))

# # find the schema classes that inherit from a given class
# inh_classes = schema_explorer.find_child_classes("Assay")
# print("classes that inherit from class 'Assay' are: {}".format(inh_classes))

# # get all details about a specific class in the schema
# class_details = schema_explorer.explore_class(TEST_CLASS)
# print("information/details about class {} : {} ".format(TEST_CLASS, class_details))

# # get all details about a specific property in the schema
# TEST_PROP = 'increasesActivityOf'
# prop_details = schema_explorer.explore_property(TEST_PROP)
# print("information/details about property {} : {}".format(TEST_PROP, prop_details))

# # get name/label of the property associated with a given class' display name
# prop_label = schema_explorer.get_property_label_from_display_name("Basic Statistics")
# print("label of the property associated with 'Basic Statistics': {}".format(prop_label))

# # get name/label of the class associated with a given class' display name
# class_label = schema_explorer.get_property_label_from_display_name("Basic Statistics")
# print("label of the class associated with 'Basic Statistics': {}".format(class_label))

# # generate template of class in schema
# class_temp = schema_explorer.generate_class_template()
# print("generic template of a class in the schema/data model: {}".format(class_temp))

# # modified TEST_CLASS ("Sequencing") based on the above generated template
# class_mod = {
#                 "@id": "bts:Sequencing",
#                 "@type": "rdfs:Class",
#                 "rdfs:comment": "Modified Test: Module for next generation sequencing assays",
#                 "rdfs:label": "Sequencing",
#                 "rdfs:subClassOf": [
#                     {
#                         "@id": "bts:Assay"
#                     }
#                 ],
#                 "schema:isPartOf": {
#                     "@id": "http://schema.biothings.io"
#                 },
#                 "sms:displayName": "Sequencing",
#                 "sms:required": "sms:false"
#             }

# # make edits to TEST_CLASS based on the above template and pass it to edit_class() 
# schema_explorer.edit_class(class_info=class_mod)

# # verify that the comment associated with TEST_CLASS has indeed been changed
# class_details = schema_explorer.explore_class(TEST_CLASS)
# print("Modified {} details : {}".format(TEST_CLASS, class_details))
