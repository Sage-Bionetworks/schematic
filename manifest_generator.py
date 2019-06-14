from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from schema_explorer import SchemaExplorer
from schema_generator import get_JSONSchema_requirements

class ManifestGenerator(object):

    """TODO:
    Add documentation and style according to style-guide
    """

    def __init__(self,
                 se: SchemaExplorer,
                 root: str,
                 title: str
                 ) -> None:
    
        """TODO:
        Add documentation and style according to style-guide
        """

        # If modifying these scopes, delete the file token.pickle.
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']

        # credentials file path
        self.credentials_path = 'credentials.json' 

        self.root = root

        self.title = title

        self.se = se


    def buildCredentials(self):

        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.scopes)
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('sheets', 'v4', credentials=creds)

        return service


    def _columnToLetter(self, column):
         character = chr(ord('A') + column % 26)
         remainder = column // 26
         if column >= 26:
            return self._columnToLetter(remainder-1) + character
         else:
            return character


    def _createEmptyManifestSpreadsheet(self, title, service):

        # create an empty spreadsheet
        spreadsheet = {
            'properties': {
                'title': title
            }
        }   
        
        spreadsheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')

        return spreadsheet_id


    def getManifest(self): 

        service = self.buildCredentials()

        spreadsheet_id = self._createEmptyManifestSpreadsheet(self.title, service)

        json_schema = get_JSONSchema_requirements(self.se, self.root, self.title)

        required_metadata_fields = {}

        # gathering dependency requirements and corresponding allowed values constraints for root node
        for req in json_schema["required"]: 
            if req in json_schema["properties"]:
                required_metadata_fields[req] = json_schema["properties"][req]["enum"]
        print(required_metadata_fields) 
    
        # gathering dependency requirements and allowed value constraints for conditional dependencies
        for conditional_reqs in json_schema["allOf"]: 
             if "required" in conditional_reqs["if"]:
                 for req in conditional_reqs["if"]["required"]: 
                    if req in conditional_reqs["if"]["properties"]:
                        if not req in required_metadata_fields: 
                            required_metadata_fields[req] = conditional_reqs["if"]["properties"][req]["enum"]
                 print ("after conditional if") 
                 print(required_metadata_fields)
                 for req in conditional_reqs["then"]["required"]: 
                     if not req in required_metadata_fields:
                         required_metadata_fields[req] = []    

        print ("after conditional if and then") 
        print(required_metadata_fields)
        

        # adding columns
        end_col = len(required_metadata_fields.keys())
        end_col_letter = self._columnToLetter(end_col) 

        range = "Sheet1!A1:" + str(end_col_letter) + "1"
        values = [list(required_metadata_fields.keys())]
        
        body = {
                "values": values 
        }
       
        print(required_metadata_fields)

        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range, valueInputOption="RAW", body=body).execute()


        # adding valid values as dropdowns
        for i, (req, values) in enumerate(required_metadata_fields.items()):

            req_vals = [{"userEnteredValue":value} for value in values if value]

            body =  {
                      "requests": [
                        {
                        'setDataValidation':{
                            'range':{
                                'startRowIndex':1,
                                'startColumnIndex':i, 
                                'endColumnIndex':i+1, 
                            },
                            'rule':{
                                'condition':{
                                    'type':'ONE_OF_LIST', 
                                    'values': req_vals
                                },
                                'inputMessage' : 'Choose one from dropdown',
                                'strict':True,
                                'showCustomUi': True
                            }
                        }            
                    }
                ]
            }   

            response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            
            return "https://docs.google.com/spreadsheets/d/" + spreadsheet_id

