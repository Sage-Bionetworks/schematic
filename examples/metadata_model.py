import json
import os
import sys

from schematic.models.metadata import MetadataModel

from schematic.utils.config_utils import load_yaml
from definitions import ROOT_DIR, CONFIG_PATH, CREDS_PATH, DATA_PATH

# load config data from yaml file into config_data dict
config_data = load_yaml(CONFIG_PATH)

if config_data is None:
    sys.exit("Your config file may be empty.")

# path to the JSON-LD schema that is stored "locally" in the app directory
MM_LOC = os.path.join(DATA_PATH, config_data["model"]["input"]["location"])
MM_TYPE = config_data["model"]["input"]["file_type"]

# create instance of MetadataModel class
metadata_model_htan = MetadataModel(MM_LOC, MM_TYPE)
print("*****************************************************")

# TEST_COMP used for testing methods in 'metadata.py'
TEST_COMP = "FollowUp"

# testing manifest generation - manifest is generated based on a JSON schema parsed from schema.org schema, which generates a google spreadsheet.
# To generate the sheet, the backend requires Google API credentials in a file credentials.json stored locally in the same directory as this file
# this credentials file is also stored on Synapse and can be retrieved given sufficient permissions to the Synapse project

# Google API credentials file stored on Synapse
API_CREDS = config_data["synapse"]["api_creds"]

# try downloading 'credentials.json' file (if not present already)
if not os.path.exists(CREDS_PATH):

    print("Retrieving Google API credentials from Synapse..")
    import synapseclient

    syn = synapseclient.Synapse()
    syn.login()
    syn.get(API_CREDS, downloadLocation = ROOT_DIR)
    print("Stored Google API credentials.")

print("Google API credentials successfully located..")

print("*****************************************************")

# testing manifest generation from a given root node without optionally provided JSON schema
print("Testing manifest generation based on a provided schema.org schema..")
manifest_url = metadata_model_htan.getModelManifest(title="Test_" + TEST_COMP, rootNode=TEST_COMP, filenames=["1.txt", "2.txt", "3.txt"])
print(manifest_url)

print("*****************************************************")

# testing manifest generation with optionally provided JSON validation schema
print("Testing manifest generation based on an additionally provided JSON schema..")
HTAPP_VALIDATION_SCHEMA = os.path.join(DATA_PATH, config_data["model"]["demo"]["validation_file_location"])

with open(HTAPP_VALIDATION_SCHEMA, "r") as f:
    json_schema = json.load(f)

HTAPP_SCHEMA = os.path.join(DATA_PATH, config_data["model"]["demo"]["location"])
HTAPP_SCHEMA_TYPE = config_data["model"]["demo"]["file_type"]

metadata_model_htapp = MetadataModel(HTAPP_SCHEMA, HTAPP_SCHEMA_TYPE)
manifest_url = metadata_model_htapp.getModelManifest(title="Example Manifest", rootNode="", jsonSchema=json_schema)
print(manifest_url)

print("*****************************************************")

# use JSON schema object to validate the integrity of annotations in manifest file (track annotation errors)
# without optionally/additionally provided JSON schema
print("Testing metadata model-based validation..")

MANIFEST_PATH = os.path.join(DATA_PATH, config_data["model"]["demo"]["valid_manifest"])
print("Testing validation with jsonSchema generation from schema.org schema..")
annotation_errors = metadata_model_htan.validateModelManifest(MANIFEST_PATH, TEST_COMP)
print(annotation_errors)

print("*****************************************************")

# use JSON schema object to validate the integrity of annotations in manifest file (track annotation errors)
# with additionally provided JSON schema
print("Testing validation with provided JSON schema..")
annotation_errors = metadata_model_htapp.validateModelManifest(MANIFEST_PATH, TEST_COMP, json_schema)
print(annotation_errors)

print("*****************************************************")

# populate a manifest with content from an already existing/filled out manifest
print("Testing metadata model-based manifest population..")
pre_populated_manifest_url = metadata_model_htan.populateModelManifest("Test_" + TEST_COMP, MANIFEST_PATH, TEST_COMP)
print(pre_populated_manifest_url)

print("*****************************************************")

# get all the nodes that are dependent on a given node based on a specific relationship type
print("Testing metadata model-based object dependency generation..")
print("Generating dependency graph and ordering dependencies..")
dependencies = metadata_model_htan.getOrderedModelNodes(TEST_COMP, "requiresDependency")
print(dependencies)

with open(TEST_COMP + "_dependencies.json", "w") as f:
    json.dump(dependencies, f, indent = 3)

print("Dependencies stored in: " + TEST_COMP + "_dependencies.json")

print("*****************************************************")

# get all nodes that are dependent on a given component
print("Testing metadata model-based component dependency generation..")
print("Generating dependency graph and ordering dependencies..")

dependencies = metadata_model_htan.getOrderedModelNodes(TEST_COMP, "requiresComponent")
print(dependencies)

with open(TEST_COMP + "component_dependencies.json", "w") as f:
    json.dump(dependencies, f, indent = 3)

print("component dependencies stored: " + TEST_COMP + "component_dependencies.json")