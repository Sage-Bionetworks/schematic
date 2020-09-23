#!/usr/bin/env python3

import os
import json
import argparse

import pandas as pd

from schematic.schemas.explorer import SchemaExplorer
from schematic.utils.csv_utils import create_schema_classes
from schematic.utils.config_utils import load_yaml

# Try to find BioThings JSON-LD file relative to script
script_dir = os.path.dirname(os.path.realpath(__file__))
biothings  = os.path.join(script_dir, "..", "data", "schema_org_schemas",
                          "biothings.jsonld")
is_biothings_absent = not os.path.exists(biothings)

# Create command-line argument parser
parser = argparse.ArgumentParser()
parser.add_argument("schema_csv", nargs="+", help="Input CSV schema files.")
parser.add_argument("schema_jsonld", help="Output JSON-LD schema file.")
parser.add_argument("--base_schema_jsonld", "-b", default=biothings,
                    help="Input base schema JSON-LD file. Typically BioThings schema.",
                    required=is_biothings_absent, metavar="biothings.jsonld")
args = parser.parse_args()

# instantiate schema explorer
base_se = SchemaExplorer()

# load base schema (BioThings)
base_se.load_schema(args.base_schema_jsonld)

for schema_extension_csv in args.schema_csv:
    schema_extension = pd.read_csv(schema_extension_csv)
    base_se = create_schema_classes(schema_extension, base_se)

# saving updated schema.org schema
base_se.export_schema(args.schema_jsonld)
