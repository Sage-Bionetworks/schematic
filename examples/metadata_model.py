#!/usr/bin/env python3

import os
import sys
import json
import argparse
import synapseclient

from schematic.models.metadata import MetadataModel
from schematic.utils.google_api_utils import download_creds_file
from schematic import CONFIG

# Create command-line argument parser
parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument("title", metavar="TITLE", help="Title of generated manifest file.")
parser.add_argument("data_type", metavar="DATA TYPE", help="data type from the schema.org schema.")
parser.add_argument("--config", "-c", help="Configuration YAML file.")
args = parser.parse_args()

# Load configuration
config_data = CONFIG.load_config(args.config)

# path to the JSON-LD schema that is stored "locally" in the app directory
MM_LOC = CONFIG["model"]["input"]["location"]
MM_TYPE = CONFIG["model"]["input"]["file_type"]

# create instance of MetadataModel class
metadata_model = MetadataModel(MM_LOC, MM_TYPE)

# TEST_DATA_TYPE used for testing methods in `metadata.py`
TEST_DATA_TYPE = args.data_type

# testing manifest generation - manifest is generated based on a JSON schema parsed from schema.org schema, which generates a google spreadsheet.
# To generate the sheet, the backend requires Google API credentials in a file credentials.json stored locally in the same directory as this file
# this credentials file is also stored on Synapse and can be retrieved given sufficient permissions to the Synapse project

# make sure the 'credentials.json' file is downloaded and is present in the right path/location
try:
    download_creds_file()
except synapseclient.core.exceptions.SynapseHTTPError:
    print("Make sure the credentials set in the config file are correct.")

print("*****************************************************")

# testing manifest generation from a given root node without optionally provided JSON schema
print("Testing manifest generation based on a provided schema.org schema..")
manifest_url = metadata_model.getModelManifest(title=args.title, rootNode=TEST_DATA_TYPE, filenames=["1.txt", "2.txt", "3.txt"])
print(manifest_url)

print("*****************************************************")

# use JSON schema object to validate the integrity of annotations in manifest file (track annotation errors)
# without optionally/additionally provided JSON schema
print("Testing metadata model-based validation..")

MANIFEST_PATH = CONFIG["synapse"]["manifest_filename"]
print("Testing validation with jsonSchema generation from schema.org schema..")
annotation_errors = metadata_model.validateModelManifest(MANIFEST_PATH, TEST_DATA_TYPE)
print(annotation_errors)

print("*****************************************************")

# populate a manifest with content from an already existing/filled out manifest
print("Testing metadata model-based manifest population..")
pre_populated_manifest_url = metadata_model.populateModelManifest("Test_" + TEST_DATA_TYPE, MANIFEST_PATH, TEST_DATA_TYPE)
print(pre_populated_manifest_url)

print("*****************************************************")

# get all the nodes that are dependent on a given node based on a specific relationship type
print("Testing metadata model-based object dependency generation..")
print("Generating dependency graph and ordering dependencies..")
dependencies = metadata_model.getOrderedModelNodes(TEST_DATA_TYPE, "requiresDependency")
print(dependencies)

with open(TEST_DATA_TYPE + "_dependencies.json", "w") as f:
    json.dump(dependencies, f, indent = 3)

print("Dependencies stored in: " + TEST_DATA_TYPE + "_dependencies.json")

print("*****************************************************")

# get all nodes that are dependent on a given data type
print("Testing metadata model-based data type dependency generation..")
print("Generating dependency graph and ordering dependencies..")

dependencies = metadata_model.getOrderedModelNodes(TEST_DATA_TYPE, "requiresComponent")
print(dependencies)

with open(TEST_DATA_TYPE + "data_type_dependencies.json", "w") as f:
    json.dump(dependencies, f, indent = 3)

print("data type dependencies stored: " + TEST_DATA_TYPE + "data_type_dependencies.json")