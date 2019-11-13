import os
import json

import pandas as pd

from schema_explorer import SchemaExplorer

from csv_2_schemaorg import create_schema_classes 


# path to base schema
base_schema_path = "./schemas/biothings_schema.jsonld"

# schema name (used to name schema json-ld file as well)
output_schema_name = "HTAN"

# schema extension definition csv files
schema_extensions_csv = [
                        #"./schemas/scRNA-seq.csv",
                        "./schemas/core.csv"
                        ]

# instantiate schema explorer
base_se = SchemaExplorer()

# load base schema (BioThings)
base_se.load_schema(base_schema_path)

for schema_extension_csv in schema_extensions_csv:
    schema_extension = pd.read_csv(schema_extension_csv)    
    base_se = create_schema_classes(schema_extension, base_se)
    

# saving updated schema.org schema
base_se.export_schema(os.path.join(os.path.dirname(schema_path), output_schema_name + ".jsonld"))
