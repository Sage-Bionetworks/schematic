from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
import networkx as nx

dmr = DataModelRelationships()

model_path = "tests/data/example.model_column_type.jsonld"

parser = DataModelParser(model_path)
data_model_dict = parser.parse_model()

string_type_component = data_model_dict.get("Check String")
assert "Relationships" in string_type_component
relationships_dict = string_type_component.get("Relationships")
assert "ColumnType" in relationships_dict
string_dict_column_type = relationships_dict.get("ColumnType")
assert string_dict_column_type == "string"
print(f"String type column type from parsed model dict: {string_dict_column_type}")

data_model_grapher = DataModelGraph(data_model_dict, "class_label")
graph_data_model = data_model_grapher.graph
print(graph_data_model.nodes["CheckString"])
dmge = DataModelGraphExplorer(graph_data_model)
assert dmge.get_node_column_type("CheckString") == "string"
assert "CheckString" in graph_data_model.nodes
node_dict = graph_data_model.nodes["CheckString"]
assert "columnType" in node_dict
column_type = node_dict["columnType"]
assert column_type == "string"

column_types_from_graph = nx.get_node_attributes(graph_data_model, "columnType")
string_graph_column_type = column_types_from_graph.get("CheckString")
assert (
    string_graph_column_type is not None
), f"Expected 'CheckString' to have a columnType attribute, but got {string_graph_column_type}"
