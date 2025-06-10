from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
import networkx as nx

dmr = DataModelRelationships()

model_path = "tests/data/example.model.column_type_component.csv"

parser = DataModelParser(model_path)
data_model_dict = parser.parse_model()
print(data_model_dict["String type"])

string_type_component = data_model_dict.get("String type")
assert "Relationships" in string_type_component
relationships_dict = string_type_component.get("Relationships")
assert "ColumnType" in relationships_dict

string_dict_column_type = relationships_dict.get("ColumnType")
assert string_dict_column_type == "string"
print(f"String type column type from parsed model dict: {string_dict_column_type}")

data_model_grapher = DataModelGraph(data_model_dict, "class_label")
graph_data_model = data_model_grapher.graph

dmge = DataModelGraphExplorer(graph_data_model)
assert dmge.get_node_column_type("Stringtype") == "string"
assert "Stringtype" in graph_data_model.nodes
node_dict = graph_data_model.nodes["Stringtype"]
assert "columnType" in node_dict
column_type = node_dict["columnType"]
assert column_type == "string"

column_types_from_graph = nx.get_node_attributes(graph_data_model, "columnType")
string_graph_column_type = column_types_from_graph.get("Stringtype")
assert (
    string_graph_column_type is not None
), f"Expected 'Stringtype' to have a columnType attribute, but got {string_graph_column_type}"
