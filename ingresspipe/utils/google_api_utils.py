import os
import synapseclient

from ingresspipe.config.config import storage

# synapse ID of the 'credentials.json' file, which we need in order to establish communication with gAPIs/services
SYN_CREDS = storage["Synapse"]["api_creds"]

SYN_UNAME = storage["Synapse"]["username"]
SYN_PWD = storage["Synapse"]["password"]

def download_creds_file():
    if not os.path.exists("./credentials.json"):
    
        print("Retrieving Google API credentials from Synapse...")
        syn = synapseclient.Synapse()
        syn.login(SYN_UNAME, SYN_PWD, rememberMe=False)
        syn.get(SYN_CREDS, downloadLocation = "./")
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