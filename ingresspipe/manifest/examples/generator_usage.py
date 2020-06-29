import synapseclient
from ingresspipe.manifest.generator import ManifestGenerator

from ingresspipe.utils.google_api_utils import download_creds_file

PATH_TO_JSONLD = "./data/schema_org_schemas/HTAN.jsonld"

# create an instance of ManifestGenerator class
TEST_NODE = "FollowUp"
manifest_generator = ManifestGenerator(title="Demo Manifest", path_to_json_ld=PATH_TO_JSONLD, root=TEST_NODE)

# make sure the 'credentials.json' file is downloaded and is present in the right path/location
try:
    download_creds_file()
except synapseclient.core.exceptions.SynapseHTTPError:
    print("Make sure the credentials set in the config file are correct.")

# get manifest (csv) url
manifest_generator.get_manifest()