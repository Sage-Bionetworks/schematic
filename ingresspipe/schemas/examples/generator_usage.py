from schema_generator.schema_generator_module import SchemaGenerator

PATH_TO_JSON_LD = "./schemas/HTAN.jsonld"

if __name__ == "__main__":
    schema_generator = SchemaGenerator(PATH_TO_JSON_LD)

    node_name = "Diagnosis"
    schema_name = "diagnosis-schema"

    try:
        json_schema = schema_generator.get_json_schema_requirements(node_name, "diagnosis-schema")
        print(json_schema)
    except IndexError:
        print("Please enter a valid node name.")