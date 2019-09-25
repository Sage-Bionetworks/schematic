from __future__ import print_function
import pickle
import os.path

import pandas as pd

from typing import Any, Dict, Optional, Text

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pygsheets as ps

from schema_explorer import SchemaExplorer
from schema_generator import get_JSONSchema_requirements

class ManifestGenerator(object):

    """TODO:
    Add documentation and style according to style-guide
    """

    def __init__(self,
                 se: SchemaExplorer,
                 root: str,
                 title: str,
                 additionalMetadata: Dict
                 ) -> None:
    
        """TODO: read in a config file instead of hardcoding paths to credential files...
        """

        # If modifying these scopes, delete the file token.pickle.
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

        # credentials file path
        self.credentials_path = 'credentials.json'

        # google service for Drive API
        self.driveService = None

        # google service for Sheet API
        self.sheetService = None

        # google service credentials object
        self.creds = None

        # schema root
        self.root = root

        # manifest title
        self.title = title

        # schema explorer object
        self.se = se

        # additional metadata to add to manifest
        self.additionalMetadata = additionalMetadata


    # TODO: replace by pygsheets calls (need to verify driver service can be supported)?
    def buildCredentials(self):

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
                creds = flow.run_console() ### don't have to deal with ports
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        self.sheetService = build('sheets', 'v4', credentials=creds)
        self.driveService = build('drive', 'v3', credentials=creds)
        self.creds = creds
        
        return


    def _columnToLetter(self, column):
         character = chr(ord('A') + column % 26)
         remainder = column // 26
         if column >= 26:
            return self._columnToLetter(remainder-1) + character
         else:
            return character


    def _createEmptyManifestSpreadsheet(self, title):

        # create an empty spreadsheet
        spreadsheet = {
            'properties': {
                'title': title
            }
        }   
        
        spreadsheet = self.sheetService.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
        spreadsheetId = spreadsheet.get('spreadsheetId')

        return spreadsheetId


    def _setPermissions(self, fileId):

        def callback(request_id, response, exception):
            if exception:
                # Handle error
                print(exception)
            else:
                print ("Permission Id: %s" % response.get('id'))

        batch = self.driveService.new_batch_http_request(callback = callback)
       
        worldPermission = {
                            'type': 'anyone',
                            'role': 'writer'
        }

        batch.add(self.driveService.permissions().create(
                                        fileId = fileId,
                                        body = worldPermission,
                                        fields = 'id',
                                        )
        )
        batch.execute()


    def getManifest(self): 

        self.buildCredentials()

        spreadsheetId = self._createEmptyManifestSpreadsheet(self.title)

        json_schema = get_JSONSchema_requirements(self.se, self.root, self.title)

        required_metadata_fields = {}

        # gathering dependency requirements and corresponding allowed values constraints for root node
        for req in json_schema["required"]: 
            if req in json_schema["properties"]:
                required_metadata_fields[req] = json_schema["properties"][req]["enum"]
            else:
                required_metadata_fields[req] = []
   

        # gathering dependency requirements and allowed value constraints for conditional dependencies if any
        if "allOf" in json_schema:
            
            for conditional_reqs in json_schema["allOf"]: 
                 if "required" in conditional_reqs["if"]:
                     for req in conditional_reqs["if"]["required"]: 
                        if req in conditional_reqs["if"]["properties"]:
                            if not req in required_metadata_fields:
                                if req in json_schema["properties"]:
                                    required_metadata_fields[req] = json_schema["properties"][req]["enum"]
                                else:
                                    required_metadata_fields[req] = conditional_reqs["if"]["properties"][req]["enum"]
                    
                     for req in conditional_reqs["then"]["required"]: 
                         if not req in required_metadata_fields:
                                if req in json_schema["properties"]:
                                    required_metadata_fields[req] = json_schema["properties"][req]["enum"]
                                else:
                                     required_metadata_fields[req] = []    

        # if additional metadata is provided append columns (if those do not exist already

        if self.additionalMetadata:
            for column in self.additionalMetadata.keys():
                if not column in required_metadata_fields:
                    required_metadata_fields[column] = []
    

        # adding columns
        end_col = len(required_metadata_fields.keys())
        end_col_letter = self._columnToLetter(end_col) 

        range = "Sheet1!A1:" + str(end_col_letter) + "1"
        values = [list(required_metadata_fields.keys())]
        
        body = {
                "values": values 
        }

       
        self.sheetService.spreadsheets().values().update(spreadsheetId=spreadsheetId, range=range, valueInputOption="RAW", body=body).execute()

        # adding additinoal metadata values if needed and adding value-constraints from data model as dropdowns
        for i, (req, values) in enumerate(required_metadata_fields.items()):

            #adding additional metadata if needed
            if self.additionalMetadata and req in self.additionalMetadata:

                values = self.additionalMetadata[req]
                target_col_letter = self._columnToLetter(i) 

                body =  {
                            "majorDimension":"COLUMNS",
                            "values":[values]
                }
                
                response = self.sheetService.spreadsheets().values().update(spreadsheetId=spreadsheetId, range = target_col_letter + '2:' + target_col_letter + str(len(values) + 1), valueInputOption = "RAW", body = body).execute()

                continue


            # adding value-constraints if any
            req_vals = [{"userEnteredValue":value} for value in values if value]

            if not req_vals:
                continue

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

            response = self.sheetService.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=body).execute()
    
        # setting up spreadsheet permissions (setup so that anyone with the link can edit)
        self._setPermissions(spreadsheetId)

        manifestUrl = "https://docs.google.com/spreadsheets/d/" + spreadsheetId
     
        print("==========================")
        print("Manifest successfully generated from schema!")
        print("URL: " + manifestUrl)
        print("==========================")
        

        return manifestUrl


    def populateManifestSpreasheet(self, existingManifestPath, emptyManifestURL):

        manifest = pd.read_csv(existingManifestPath).fillna("")
        self.buildCredentials()
        gc = ps.authorize(custom_credentials = self.creds)
        sh = gc.open_by_url(emptyManifestURL)
        wb = sh[0]
        wb.set_dataframe(manifest, (1,1))

        return sh.url 

