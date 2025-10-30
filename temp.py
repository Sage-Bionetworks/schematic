from schematic.schemas.create_json_schema import create_json_schema
from schematic.schemas.data_model_graph import create_data_model_graph_explorer

#dmge = create_data_model_graph_explorer("tests/data/example.model.csv")
#x = create_json_schema(dmge=dmge,datatype="Patient", schema_name="test", schema_path="x.json")

dmge = create_data_model_graph_explorer("/home/alamb/Downloads/ark.biospecimen_model.csv")
x = create_json_schema(dmge=dmge,datatype="BiospecimenMetadataTemplate", schema_name="test", schema_path="x.json")