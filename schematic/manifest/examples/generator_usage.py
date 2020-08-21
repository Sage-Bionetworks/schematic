import synapseclient
import os

from schematic.manifest.generator import ManifestGenerator

from schematic.utils.google_api_utils import download_creds_file
from schematic.utils.config_utils import load_yaml

from definitions import CONFIG_PATH, DATA_PATH

config_data = load_yaml(CONFIG_PATH)

PATH_TO_JSONLD = os.path.join(DATA_PATH, config_data["model"]["input"]["location"])

# create an instance of ManifestGenerator class
TEST_NODE = "FollowUp"
manifest_generator = ManifestGenerator(title="FollowUp Manifest", path_to_json_ld=PATH_TO_JSONLD, root=TEST_NODE)

# make sure the 'credentials.json' file is downloaded and is present in the right path/location
try:
    download_creds_file()
except synapseclient.core.exceptions.SynapseHTTPError:
    print("Make sure the credentials set in the config file are correct.")

# get manifest (csv) url
print(manifest_generator.get_manifest())
