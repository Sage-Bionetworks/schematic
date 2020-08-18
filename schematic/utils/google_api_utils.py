import os
import synapseclient

from schematic.utils.config_utils import load_yaml
from definitions import ROOT_DIR, CONFIG_PATH, CREDS_PATH

config_data = load_yaml(CONFIG_PATH)

# synapse ID of the 'credentials.json' file, which we need in order to establish communication with gAPIs/services
SYN_CREDS = config_data["synapse"]["api_creds"]

def download_creds_file():
    if not os.path.exists(CREDS_PATH):
    
        print("Retrieving Google API credentials from Synapse...")
        syn = synapseclient.Synapse()
        syn.login()
        syn.get(SYN_CREDS, downloadLocation = ROOT_DIR)
        print("Downloaded Google API credentials file.")

def execute_google_api_requests(service, requests_body, **kwargs):
    """
    Execute google API requests batch; attempt to execute in parallel.

    Args:
        service: google api service; for now assume google sheets service that is instantiated and authorized
        service_type: default batchUpdate; TODO: add logic for values update
        kwargs: google API service parameters
    Return: google API response
    """

    if "spreadsheet_id" in kwargs and "service_type" in kwargs and kwargs["service_type"] == "batch_update":
        # execute all requests
        response = service.spreadsheets().batchUpdate(spreadsheetId=kwargs["spreadsheet_id"], body = requests_body).execute()
        
        return response