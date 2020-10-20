import os
import argparse
import synapseclient

from schematic.manifest.generator import ManifestGenerator
from schematic.utils.google_api_utils import download_creds_file
from schematic import CONFIG

# Create command-line argument parser
parser = argparse.ArgumentParser()
parser.add_argument("--config", "-c", help="Configuration YAML file.")
args = parser.parse_args()

# Load configuration
config_data = CONFIG.load_config(args.config)

# path to schema.org/JSON-LD schema ass specified in `config.yml`
PATH_TO_JSONLD = CONFIG["model"]["input"]["location"]

# make sure the 'credentials.json' file is downloaded and is present in the right path/location
try:
    download_creds_file()
except synapseclient.core.exceptions.SynapseHTTPError:
    print("Make sure the credentials set in the config file are correct.")

# create an instance of ManifestGenerator class
TEST_NODE = "FollowUp"
manifest_generator = ManifestGenerator(title="FollowUp Manifest", path_to_json_ld=PATH_TO_JSONLD, root=TEST_NODE)

# get manifest (csv) url
print(manifest_generator.get_manifest())