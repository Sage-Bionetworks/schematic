import os
import synapseclient
import pickle
import pygsheets as ps

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from schematic import CONFIG

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# it will create 'token.pickle' based on credentials.json
# TODO: replace by pygsheets calls?
def build_credentials() -> dict:
    creds = None
    # The file token.pickle stores the user's access and refresh tokens,
    # and is created automatically when the authorization flow completes for the first time.
    if os.path.exists(CONFIG.TOKEN_PICKLE):
        with open(CONFIG.TOKEN_PICKLE, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CONFIG.CREDS_PATH, SCOPES)
            creds = flow.run_console() ### don't have to deal with ports
        # Save the credentials for the next run
        with open(CONFIG.TOKEN_PICKLE, 'wb') as token:
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
    credentials = service_account.Credentials.from_service_account_file(CONFIG.SERVICE_ACCT_CREDS, scopes=SCOPES)

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
    if not os.path.exists(CONFIG.CREDS_PATH):

        print("Retrieving Google API credentials from Synapse...")
        # synapse ID of the 'credentials.json' file, which we need in
        # order to establish communication with gAPIs/services
        API_CREDS = CONFIG["synapse"]["api_creds"]
        syn = synapseclient.Synapse()
        syn.login()
        # Download in parent directory of CREDS_PATH to
        # ensure same file system for os.rename()
        creds_dir = os.path.dirname(CONFIG.CREDS_PATH)
        creds_file = syn.get(API_CREDS, downloadLocation = creds_dir)
        os.rename(creds_file.path, CONFIG.CREDS_PATH)
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