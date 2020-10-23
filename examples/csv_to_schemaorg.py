#!/usr/bin/env python3

import os
import re
import json
import argparse
import pandas as pd

from schematic.schemas.explorer import SchemaExplorer
from schematic.utils.csv_utils import create_schema_classes
from schematic import CONFIG

# Constants (to avoid magic numbers)
FIRST = 0

# Create command-line argument parser
parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument("schema_csv_list", nargs="+", metavar="schema_csv", help="Input CSV schema files.")
parser.add_argument("--output_jsonld", "-o", help="Output JSON-LD schema file.")
parser.add_argument("--config", "-c", help="Configuration YAML file.")
args = parser.parse_args()

# Load configuration
config_data = CONFIG.load_config(args.config)
base_schema_path = CONFIG["model"]["biothings"]["location"]

# instantiate schema explorer
base_se = SchemaExplorer()

# load base schema (BioThings)
base_se.load_schema(base_schema_path)

for schema_extension_csv in args.schema_csv_list:
    schema_extension = pd.read_csv(schema_extension_csv)
    base_se = create_schema_classes(schema_extension, base_se)

# Default to outputting the JSON-LD alongside the first input CSV
if args.output_jsonld is None:
    # Default to first CSV since most users will only have one input CSV file
    first_csv = args.schema_csv_list[FIRST]
    first_csv_noext = re.sub("[.]csv$", "", first_csv)
    args.output_jsonld = first_csv_noext + ".jsonld"
    print("By default, the JSON-LD output will be stored alongside the first "
          "input CSV file. In this case, it will appear here: '%s'. You can "
          "use the `--output_jsonld` argument to specify another file path."
          % args.output_jsonld)

# saving updated schema.org schema
base_se.export_schema(args.output_jsonld)