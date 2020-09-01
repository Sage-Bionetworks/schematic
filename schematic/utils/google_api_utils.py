import os
import synapseclient
import pickle
import pygsheets as ps

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from schematic.utils.config_utils import load_yaml
from definitions import ROOT_DIR, CONFIG_PATH, CREDS_PATH, TOKEN_PICKLE, CREDS_PATH, SERVICE_ACCT_CREDS

config_data = load_yaml(CONFIG_PATH)

# synapse ID of the 'credentials.json' file, which we need in order to establish communication with gAPIs/services
SYN_CREDS = config_data["synapse"]["api_creds"]

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# it will create 'token.pickle' based on credentials.json
# TODO: replace by pygsheets calls?
def build_credentials() -> dict:
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, 
    # and is created automatically when the authorization flow completes for the first time.
    if os.path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
            creds = flow.run_console() ### don't have to deal with ports
        # Save the credentials for the next run
        with open(TOKEN_PICKLE, 'wb') as token:
            pickle.dump(creds, token)

    # get a Google Sheet API service
    sheet_service = build('sheets', 'v4', credentials=creds)
    # get a Google Drive API service
    drive_service = build('drive', 'v3', credentials=creds)
    
    return {
        'sheet_service': sheet_service,
        'drive_service': drive_service,
        'creds': creds
    }

def build_service_account_creds():
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCT_CREDS, scopes=SCOPES)

    # get a Google Sheet API service
    sheet_service = build('sheets', 'v4', credentials=credentials)
    # get a Google Drive API service
    drive_service = build('drive', 'v3', credentials=credentials)
    
    return {
        'sheet_service': sheet_service,
        'drive_service': drive_service,
        'creds': credentials
    }

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