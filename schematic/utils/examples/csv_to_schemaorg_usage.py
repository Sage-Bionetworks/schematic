import os
import json

import pandas as pd

from schematic.schemas.explorer import SchemaExplorer

from schematic.utils.csv_utils import create_schema_classes
from schematic.utils.config_utils import load_yaml

from definitions import CONFIG_PATH, DATA_PATH

config_data = load_yaml(CONFIG_PATH)

# path to base schema
base_schema_path = os.path.join(DATA_PATH, '', config_data["model"]["biothings"]["location"])

# schema name (used to name schema json-ld file as well)
output_schema_name = "example"

# schema extension definition csv files
schema_extensions_csv = [
                        os.path.join(DATA_PATH, '', 'csv/example.csv')
                        ]

# instantiate schema explorer
base_se = SchemaExplorer()

# load base schema (BioThings)
base_se.load_schema(base_schema_path)

for schema_extension_csv in schema_extensions_csv:
    schema_extension = pd.read_csv(schema_extension_csv)
    base_se = create_schema_classes(schema_extension, base_se)

# saving updated schema.org schema
base_se.export_schema(os.path.join(os.path.dirname(base_schema_path), output_schema_name + ".jsonld"))