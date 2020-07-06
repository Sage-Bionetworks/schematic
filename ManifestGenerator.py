from __future__ import print_function
import pickle
import os.path
import collections

import pandas as pd

from typing import Any, Dict, Optional, Text

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pygsheets as ps

from schema_explorer import SchemaExplorer
import schema_generator as sg

from utils import execute_google_api_requests

from config import style

class ManifestGenerator(object):

    """TODO:
    Add documentation and style according to style-guide
    """

    def __init__(self,
                 title: str, # manifest sheet title
                 se: SchemaExplorer = None,
                 root: str = None,
                 additional_metadata: Dict = None 
                 ) -> None:
    
        """TODO: read in a config file instead of hardcoding paths to credential files...
        """

        # If modifying these scopes, delete the file token.pickle.
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

        # path to Google API credentials file
        self.credentials_path = "credentials.json"

        # google service for Drive API
        self.drive_service = None

        # google service for Sheet API
        self.sheet_service = None

        # google service credentials object
        self.creds = None

        # schema root
        self.root = root

        # manifest title
        self.title = title

        # schema explorer object
        self.se = se

        # additional metadata to add to manifest
        self.additional_metadata = additional_metadata


    # TODO: replace by pygsheets calls?
    def build_credentials(self):

        creds = None
# The file token.pickle stores the user's access and refresh tokens, and is created automatically when the authorization flow completes for the first time.
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

        # get a Google Sheet API service
        self.sheet_service = build('sheets', 'v4', credentials=creds)
        # get a Google Drive API service
        self.drive_service = build('drive', 'v3', credentials=creds)
        self.creds = creds
        
        return

    def _gdrive_copy_file(self, origin_file_id, copy_title):
        """Copy an existing file.

        Args:
            origin_file_id: ID of the origin file to copy.
            copy_title: Title of the copy.

        Returns:
            The copied file if successful, None otherwise.
        """
        copied_file = {'name': copy_title}

        # return new copy sheet ID
        return self.drive_service.files().copy(fileId = origin_file_id, body = copied_file).execute()["id"]


    def _attribute_to_letter(self, attribute, manifest_fields):
        """Map attribute to column letter in a google sheet
        """

        # find index of attribute in manifest field
        column_idx = index(attribute, manifest_fields)

        # return the google sheet letter representation of the column index
        return self._column_to_letter(column_idx)

    def _column_to_letter(self, column):
         """Find google sheet letter representation of a column index integer
         """
         character = chr(ord('A') + column % 26)
         remainder = column // 26
         if column >= 26:
            return self._column_to_letter(remainder-1) + character
         else:
            return character


    def _columns_to_sheet_ranges(self, column_idxs):
        """map a set of column indexes to a set of Google sheet API ranges: each range includes exactly one column
        """
        ranges = []

        for column_idx in column_idxs:
            col_range = {
                        "startColumnIndex":column_idx,
                        "endColumnIndex": column_idx + 1
            }

            ranges.append(col_range)

        return ranges
            

    def _column_to_cond_format_eq_rule(self, column_idx:int, condition_argument:str, required:bool = False) -> dict:
        """Given a column index and an equality argument (e.g. one of valid values for the given column fields), generate a conditional formatting rule based on a custom formula encoding the logic: 

        'if a cell in column idx is equal to condition argument, then set specified formatting'
        """
        
        col_letter = self._column_to_letter(column_idx)

        if not required:
           bg_color = style["googleManifest"]["optBgColor"] 
        else:
           bg_color = style["googleManifest"]["reqBgColor"]
        
        boolean_rule =  {
                        "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [
                                    {
                                        "userEnteredValue": '=$' + col_letter + '1 = "' + condition_argument + '"'
                                    }
                                  ]
                        },
                        "format":{
                            'backgroundColor': bg_color 
                        }
        }
    
        return boolean_rule


    def _create_empty_manifest_spreadsheet(self, title):

        if style["googleManifest"]["masterTemplateId"]:

            # if provided with a template manifest google sheet, use it
            spreadsheet_id = self._gdrive_copy_file(style["googleManifest"]["masterTemplateId"], title)

        else:
            # if no template, create an empty spreadsheet
            spreadsheet = self.sheet_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')

        return spreadsheet_id


    def _set_permissions(self, fileId):

        def callback(request_id, response, exception):
            if exception:
                # Handle error
                print(exception)
            else:
                print ("Permission Id: %s" % response.get('id'))

        batch = self.drive_service.new_batch_http_request(callback = callback)
       
        worldPermission = {
                            'type': 'anyone',
                            'role': 'writer'
        }

        batch.add(self.drive_service.permissions().create(
                                        fileId = fileId,
                                        body = worldPermission,
                                        fields = 'id',
                                        )
        )
        batch.execute()


    def get_manifest(self, json_schema = None): 

        # TODO: Refactor get_manifest method
        # - abstract function for requirements gathering
        # - abstract google sheet API requests as functions
        # --- specifying row format
        # --- setting valid values in dropdowns for columns/cells
        # --- setting notes/comments to cells
      
        self.build_credentials()

        spreadsheet_id = self._create_empty_manifest_spreadsheet(self.title)

        if not json_schema:
            # if no json schema is provided; there must be
            # schema explorer defined for schema.org schema
            # o.w. this will throw an error
            # TODO: catch error
            json_schema = sg.get_JSONSchema_requirements(self.se, self.root, self.title)

        required_metadata_fields = {}

        # gathering dependency requirements and corresponding allowed values constraints for root node
        for req in json_schema["properties"].keys():
            if not "enum" in json_schema["properties"][req]:
                # if no valid/allowed values specified
                json_schema["properties"][req]["enum"] = []
            
            required_metadata_fields[req] = json_schema["properties"][req]["enum"]


        # gathering dependency requirements and allowed value constraints for conditional dependencies if any
        if "allOf" in json_schema: 
            for conditional_reqs in json_schema["allOf"]: 
                 if "required" in conditional_reqs["if"]:
                     for req in conditional_reqs["if"]["required"]: 
                        if req in conditional_reqs["if"]["properties"]:
                            if not req in required_metadata_fields:
                                if req in json_schema["properties"]:
                                    if not "enum" in json_schema["properties"][req]:
                                        # if no valid/allowed values specified
                                        json_schema["properties"][req]["enum"] = []
                                    required_metadata_fields[req] = json_schema["properties"][req]["enum"]
                                else:
                                    required_metadata_fields[req] = conditional_reqs["if"]["properties"][req]["enum"] if "enum" in conditional_reqs["if"]["properties"][req] else []                   
                     for req in conditional_reqs["then"]["required"]: 
                         if not req in required_metadata_fields:
                                if req in json_schema["properties"]:
                                    required_metadata_fields[req] = json_schema["properties"][req]["enum"] if "enum" in json_schema["properties"][req] else []
                                else:
                                     required_metadata_fields[req] = []    

        # if additional metadata is provided append columns (if those do not exist already)
        if self.additional_metadata:
            for column in self.additional_metadata.keys():
                if not column in required_metadata_fields:
                    required_metadata_fields[column] = []
    
        # if 'component' is in column set (see your input jsonld schema for definition of 'component', if the 'component' attribute is present), add the root node as an additional metadata component entry 
        if 'Component' in required_metadata_fields.keys():
            # check if additional metadata has actually been instantiated in the constructor (it's optional)
            # if not, instantiate it
            if not self.additional_metadata:
                self.additional_metadata = {}

            self.additional_metadata['Component'] = [self.root]

        # adding columns to manifest sheet
        end_col = len(required_metadata_fields.keys())
        end_col_letter = self._column_to_letter(end_col) 

        range = "Sheet1!A1:" + str(end_col_letter) + "1"
        ordered_metadata_fields = [list(required_metadata_fields.keys())]

        # order columns header (since they are generated based on a json schema, which is a dict)
        ordered_metadata_fields[0] = self.sort_manifest_fields(ordered_metadata_fields[0]) 
        body = {
                "values": ordered_metadata_fields
        }
        self.sheet_service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range, valueInputOption="RAW", body=body).execute()

        # format column header row
        header_format_body = {
                "requests":[
                {
                      "repeatCell": {
                        "range": {
                          "startRowIndex": 0,
                          "endRowIndex": 1
                        },
                        "cell": {
                          "userEnteredFormat": {
                            "backgroundColor": {
                              "red": 224.0/255,
                              "green": 224.0/255,
                              "blue": 224.0/255
                            },
                            "horizontalAlignment" : "CENTER",
                            "textFormat": {
                              "foregroundColor": {
                                "red": 0.0/255,
                                "green": 0.0/255,
                                "blue": 0.0/255 
                              },
                              "fontSize": 8,
                              "bold": True
                            }
                          }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                      }
                    },
                    {
                      "updateSheetProperties": {
                        "properties": {
                          "gridProperties": {
                            "frozenRowCount": 1
                          }
                        },
                        "fields": "gridProperties.frozenRowCount"
                      }
                    },
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "dimension": "COLUMNS",
                                "startIndex": 0
                            }
                        } 
                    }
                ]
        }

        response = self.sheet_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=header_format_body).execute()

        # adding additional metadata values if needed
        # adding value-constraints from data model as dropdowns
        
        #store all requests to execute at once
        requests_body = {}
        requests_body["requests"] = []
        for i, req in enumerate(ordered_metadata_fields[0]):
            values = required_metadata_fields[req]
            #adding additional metadata if needed
            if self.additional_metadata and req in self.additional_metadata:
                values = self.additional_metadata[req]
                target_col_letter = self._column_to_letter(i) 
                body =  {
                            "majorDimension":"COLUMNS",
                            "values":[values]
                }                
                response = self.sheet_service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range = target_col_letter + '2:' + target_col_letter + str(len(values) + 1), valueInputOption = "RAW", body = body).execute()


            # adding description to headers
            # this is not executed if only JSON schema is defined
            # TODO: abstract better and document
            
            # also formatting required columns
            if self.se:
                
                # get node definition
                note = sg.get_node_definition(self.se, req)

                notes_body =  {
                            "requests":[
                                {
                                    "updateCells": {
                                    "range": {
                                        "startRowIndex": 0,
                                        "endRowIndex": 1,
                                        "startColumnIndex": i,
                                        "endColumnIndex": i+1
                                    },
                                   "rows": [
                                      {
                                        "values": [
                                          {
                                            "note": note
                                          }
                                        ]
                                      }
                                    ],
                                    "fields": "note"
                                    }
                                }
                            ]
                }
                
                requests_body["requests"].append(notes_body["requests"])

            # update background colors so that columns that are required are highlighted
            # check if attribute is required and set a corresponding color
            if req in json_schema["required"]:
                bg_color = style["googleManifest"]["reqBgColor"]

                req_format_body = {
                        "requests":[
                            {
                                "repeatCell": {
                                    "range": {
                                      "startColumnIndex": i,
                                      "endColumnIndex": i+1
                                    },
                                    "cell": {
                                      "userEnteredFormat": {
                                        "backgroundColor": bg_color
                                      }
                                    },
                                    "fields": "userEnteredFormat(backgroundColor)"
                                  }
                            }
                        ]
                }
                
                requests_body["requests"].append(req_format_body["requests"])

            # adding value-constraints if any
            req_vals = [{"userEnteredValue":value} for value in values if value]
            
            if not req_vals:
                continue

            if len(req_vals) > 499:
                print("WARNING: Value range > Google Sheet limit of 500. Truncating...")
                req_vals = req_vals[:499]


            # generating sheet api request to populate the dropdown
            validation_body =  {
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

            requests_body["requests"].append(validation_body["requests"])
            
            # generate a conditional format rule for each required value (i.e. valid value) 
            # for this field (i.e. if this field is set to a valid value that may require additional
            # fields to be filled in, these additional fields will be formatted in a custom style (e.g. red background) 
            for req_val in req_vals:
                # get this required/valid value's node label in schema, based on display name (i.e. shown to the user in a dropdown to fill in)
                req_val = req_val["userEnteredValue"]
              
                req_val_node_label = sg.get_node_label(self.se, req_val)
                if not req_val_node_label:
                    # if this node is not in the graph
                    # continue - there are no dependencies for it
                    continue

                # check if this required/valid value has additional dependency attributes
                val_dependencies = sg.get_node_dependencies(self.se, req_val_node_label, schema_ordered = False)

                # prepare request calls
                dependency_formatting_body = {
                        "requests": []
                }

                if val_dependencies:
                    # if there are additional attribute dependencies find the corresponding
                    # fields that need to be filled in and construct conditional formatting rules
                    # indicating the dependencies need to be filled in
                    
                        
                    # set target ranges for this rule
                    # i.e. dependency attribute columns that will be formatted

                    # find dependency column indexes
                    # note that dependencies values must be in index 
                    # TODO: catch value error that shouldn't happen
                    column_idxs = [ordered_metadata_fields[0].index(val_dep) for val_dep in val_dependencies]

                    # construct ranges based on dependency column indexes
                    rule_ranges = self._columns_to_sheet_ranges(column_idxs)
                    # go over valid value dependencies
                    for j,val_dep in enumerate(val_dependencies):
                        is_required = False
                        
                        if sg.is_node_required(self.se, val_dep):
                            is_required = True
                        else:
                            is_required = False
                        
                        # construct formatting rule
                        formatting_rule = self._column_to_cond_format_eq_rule(i, req_val, required = is_required)

                        # construct conditional format rule 
                        conditional_format_rule = {
                              "addConditionalFormatRule": {
                                "rule": {
                                  "ranges": rule_ranges[j],
                                  "booleanRule": formatting_rule,
                                 },
                                "index": 0
                              }
                        }
                        dependency_formatting_body["requests"].append(conditional_format_rule)
                  
                # check if dependency formatting rules have been added and update sheet if so
                if dependency_formatting_body["requests"]:
                    requests_body["requests"].append(dependency_formatting_body["requests"])
                
        execute_google_api_requests(self.sheet_service, requests_body, service_type = "batch_update", spreadsheet_id = spreadsheet_id)

        # setting up spreadsheet permissions (setup so that anyone with the link can edit)
        self._set_permissions(spreadsheet_id)

        # generating spreadsheet URL
        manifest_url = "https://docs.google.com/spreadsheets/d/" + spreadsheet_id
     
        print("==========================")
        print("Manifest successfully generated from schema!")
        print("URL: " + manifest_url)
        print("==========================")
        

        return manifest_url


    def populate_manifest_spreadsheet(self, existing_manifest_path, empty_manifest_url):

        """ Creates a google sheet manifest
        based on existing manifest;  

        Args:
            existing_manifest_path: the location of the manifest containing metadata presently stored
            empty_manifest_url: the path to a manifest template to be prepopulated with existing's manifest metadata
        """        
        
        # read existing manifest
        manifest = pd.read_csv(existing_manifest_path).fillna("")

        # sort manifest columns
        manifest_fields = manifest.columns.tolist()
        manifest_fields = self.sort_manifest_fields(manifest_fields)
        manifest = manifest[manifest_fields]

        self.build_credentials()
        gc = ps.authorize(custom_credentials = self.creds)
        sh = gc.open_by_url(empty_manifest_url)
        wb = sh[0]
        wb.set_dataframe(manifest, (1,1))
        
        # set permissions so that anyone with the link can edit
        sh.share("", role = "writer", type = "anyone")

        return sh.url 


    def sort_manifest_fields(self, manifest_fields, order = "schema"):

        # order manifest fields alphabetically (base order)
        manifest_fields = sorted(manifest_fields)
        
        if order == "alphabetical":
            # if the order is alphabetical ensure that filename is first, if present
            if "Filename" in manifest_fields:
                manifest_fields.remove("Filename")
                manifest_fields.insert(0, "Filename")


        # order manifest fields based on schema (schema.org)
        if order == "schema":
            if self.se and self.root:
                # get display names of dependencies
                dependencies_display_names = sg.get_node_dependencies(self.se, self.root)

                # reorder manifest fields so that root dependencies are first and follow schema order
                manifest_fields = sorted(manifest_fields, key = lambda x: dependencies_display_names.index(x) if x in dependencies_display_names else len(manifest_fields) -1)
            
            else:
                print("No schema provided! Cannot order based on schema without a specified schema and a schema root attribute.")

        # always have entityId as last columnn, if present
        if "entityId" in manifest_fields:
            manifest_fields.remove("entityId")
            manifest_fields.append("entityId")

        print("Manifest fields:")
        print()
        print(manifest_fields)
        return manifest_fields
