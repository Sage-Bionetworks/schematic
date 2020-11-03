#!/usr/bin/env python3

import os
import argparse
import synapseclient

from schematic.manifest.generator import ManifestGenerator
from schematic.utils.google_api_utils import download_creds_file
from schematic import CONFIG

# Constants (to avoid magic numbers)
FIRST = 0

# Create command-line argument parser
parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument("title", nargs=1, metavar="TITLE", help="Title of generated manifest file.")
parser.add_argument("data_type", nargs=1, metavar="DATA TYPE", help="data type from the schema.org schema.")
parser.add_argument("--config", "-c", metavar="CONFIG", help="Configuration YAML file.")
args = parser.parse_args()

# Load configuration
config_data = CONFIG.load_config(args.config)

# path to schema.org/JSON-LD schema as specified in `config.yml`
PATH_TO_JSONLD = CONFIG["model"]["input"]["location"]

# make sure the 'credentials.json' file is downloaded and is present in the right path/location
try:
    download_creds_file()
except synapseclient.core.exceptions.SynapseHTTPError:
    print("Make sure the credentials set in the config file are correct.")

# create an instance of ManifestGenerator class
manifest_generator = ManifestGenerator(title=args.title[FIRST], path_to_json_ld=PATH_TO_JSONLD, root=args.data_type[FIRST])

# get manifest (csv) url
print(manifest_generator.get_manifest(dataset_id="syn21973647", sheet_url=False))