import os
import logging
from typing import Dict, List, Tuple
from collections import OrderedDict
import socket
from tempfile import NamedTemporaryFile

import pandas as pd
import pygsheets as ps
import json

from schematic.db.rdb import RDB
from schematic.db.sql import SQL

from schematic.schemas.explorer import SchemaExplorer
from schematic.schemas.generator import SchemaGenerator
from schematic.utils.google_api_utils import (
    build_credentials,
    execute_google_api_requests,
    build_service_account_creds,
)

from schematic.utils.io_utils import load_json
from schematic.utils.df_utils import update_df
from schematic.store.synapse import SynapseStorage

from schematic import CONFIG

logger = logging.getLogger(__name__)


class ManifestGenerator(object):
    def __init__(
        self,
        path_to_json_ld: str,  # JSON-LD file to be used for generating the manifest
        title: str = None,  # manifest sheet title
        root: str = None,
        additional_metadata: Dict = None,
        oauth: bool = True,
        use_annotations: bool = False,
    ) -> None:

        """TODO: read in a config file instead of hardcoding paths to credential files..."""

        if oauth:
            # if user wants to use OAuth for Google authentication
            # use credentials.json and create token.pickle file
            services_creds = build_credentials()
        else:
            # if not oauth then use service account credentials
            services_creds = build_service_account_creds()

        # google service for Sheet API
        self.sheet_service = services_creds["sheet_service"]

        # google service for Drive API
        self.drive_service = services_creds["drive_service"]

        # google service credentials object
        self.creds = services_creds["creds"]

        # schema root
        self.root = root

        # manifest title
        self.title = title
        if self.title is None:
            self.title = f"{self.root} - Manifest"

        # Whether to use existing annotations during manifest generation
        self.use_annotations = use_annotations

        # Warn about limited feature support for `use_annotations`
        if self.use_annotations:
            logger.warning(
                "The `use_annotations` option is currently only supported "
                "when there is no manifest file for the dataset in question."
            )
        self.path_to_json_ld = path_to_json_ld

        # SchemaGenerator() object
        self.sg = SchemaGenerator(self.path_to_json_ld)

        # additional metadata to add to manifest
        self.additional_metadata = additional_metadata

        # Determine whether current data type is file-based
        is_file_based = False
        if self.root:
            is_file_based = "Filename" in self.sg.get_node_dependencies(self.root)
        self.is_file_based = is_file_based

        # Check if manifest is being used in an RDB
        # Will want to check for central nodes with many foreign keys
        # referencing many tables
        self.rdb_root = False
        if "rdb" in path_to_json_ld.split("/")[-1].split("."):
            self.max_authority = self.sg.se.get_max_authority()
            if self.max_authority == root:
                self.rdb_root = True

    def _attribute_to_letter(self, attribute, manifest_fields):
        """Map attribute to column letter in a google sheet"""

        # find index of attribute in manifest field
        column_idx = manifest_fields.index(attribute)

        # return the google sheet letter representation of the column index
        return self._column_to_letter(column_idx)

    def _column_to_letter(self, column):
        """Find google sheet letter representation of a column index integer"""
        character = chr(ord("A") + column % 26)
        remainder = column // 26
        if column >= 26:
            return self._column_to_letter(remainder - 1) + character
        else:
            return character

    def _columns_to_sheet_ranges(self, column_idxs):
        """map a set of column indexes to a set of Google sheet API ranges: each range includes exactly one column"""
        ranges = []

        for column_idx in column_idxs:
            col_range = {
                "startColumnIndex": column_idx,
                "endColumnIndex": column_idx + 1,
            }

            ranges.append(col_range)

        return ranges

    def _column_to_cond_format_eq_rule(
        self, column_idx: int, condition_argument: str, required: bool = False
    ) -> dict:
        """Given a column index and an equality argument (e.g. one of valid values for the given column fields), generate a conditional formatting rule based on a custom formula encoding the logic:

        'if a cell in column idx is equal to condition argument, then set specified formatting'
        """

        col_letter = self._column_to_letter(column_idx)

        if not required:
            bg_color = CONFIG["style"]["google_manifest"].get(
                "opt_bg_color",
                {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 0.9019,
                },
            )
        else:
            bg_color = CONFIG["style"]["google_manifest"].get(
                "req_bg_color",
                {
                    "red": 0.9215,
                    "green": 0.9725,
                    "blue": 0.9803,
                },
            )

        boolean_rule = {
            "condition": {
                "type": "CUSTOM_FORMULA",
                "values": [
                    {
                        "userEnteredValue": "=$"
                        + col_letter
                        + '1 = "'
                        + condition_argument
                        + '"'
                    }
                ],
            },
            "format": {"backgroundColor": bg_color},
        }

        return boolean_rule

    def _gdrive_copy_file(self, origin_file_id, copy_title):
        """Copy an existing file.

        Args:
            origin_file_id: ID of the origin file to copy.
            copy_title: Title of the copy.

        Returns:
            The copied file if successful, None otherwise.
        """
        copied_file = {"name": copy_title}

        # return new copy sheet ID
        return (
            self.drive_service.files()
            .copy(fileId=origin_file_id, body=copied_file)
            .execute()["id"]
        )

    def _create_empty_manifest_spreadsheet(self, title):
        if CONFIG["style"]["google_manifest"]["master_template_id"]:

            # if provided with a template manifest google sheet, use it
            spreadsheet_id = self._gdrive_copy_file(
                CONFIG["style"]["google_manifest"]["master_template_id"], title
            )

        else:
            # if no template, create an empty spreadsheet
            spreadsheet = (
                self.sheet_service.spreadsheets()
                .create(body=spreadsheet, fields="spreadsheetId")
                .execute()
            )
            spreadsheet_id = spreadsheet.get("spreadsheetId")

        return spreadsheet_id

    def _get_cell_borders(self, cell_range):

        # set border style request
        color = {
            "red": 226.0 / 255.0,
            "green": 227.0 / 255.0,
            "blue": 227.0 / 255.0,
        }

        border_style_req = {
            "updateBorders": {
                "range": cell_range,
                "top": {"style": "SOLID", "width": 2, "color": color},
                "bottom": {"style": "SOLID", "width": 2, "color": color},
                "left": {"style": "SOLID", "width": 2, "color": color},
                "right": {"style": "SOLID", "width": 2, "color": color},
                "innerHorizontal": {"style": "SOLID", "width": 2, "color": color},
                "innerVertical": {"style": "SOLID", "width": 2, "color": color},
            }
        }

        return border_style_req

    def _set_permissions(self, fileId):
        def callback(request_id, response, exception):
            if exception:
                # Handle error
                logger.error(exception)
            else:
                logger.info(f"Permission Id: {response.get('id')}")

        batch = self.drive_service.new_batch_http_request(callback=callback)

        worldPermission = {"type": "anyone", "role": "writer"}

        batch.add(
            self.drive_service.permissions().create(
                fileId=fileId,
                body=worldPermission,
                fields="id",
            )
        )
        batch.execute()

    def _get_column_data_validation_values(
        self,
        spreadsheet_id,
        valid_values,
        column_id,
        validation_type="ONE_OF_LIST",
        custom_ui=True,
        input_message="Choose one from dropdown",
    ):

        strict = CONFIG["style"]["google_manifest"].get("strict_validation", True)

        # get valid values w/o google sheet header
        values = [valid_value["userEnteredValue"] for valid_value in valid_values]

        if validation_type == "ONE_OF_RANGE":

            # store valid values explicitly in workbook at the provided range to use as validation values
            target_col_letter = self._column_to_letter(column_id)
            body = {"majorDimension": "COLUMNS", "values": [values]}
            target_range = (
                "Sheet2!"
                + target_col_letter
                + "2:"
                + target_col_letter
                + str(len(values) + 1)
            )
            valid_values = [{"userEnteredValue": "=" + target_range}]

            response = (
                self.sheet_service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=target_range,
                    valueInputOption="RAW",
                    body=body,
                )
                .execute()
            )

        # setup validation data request body
        validation_body = {
            "requests": [
                {
                    "setDataValidation": {
                        "range": {
                            "startRowIndex": 1,
                            "startColumnIndex": column_id,
                            "endColumnIndex": column_id + 1,
                        },
                        "rule": {
                            "condition": {
                                "type": validation_type,
                                "values": valid_values,
                            },
                            "inputMessage": input_message,
                            "strict": strict,
                            "showCustomUi": custom_ui,
                        },
                    }
                }
            ]
        }

        return validation_body

    def _get_valid_values_from_jsonschema_property(self, prop: dict) -> List[str]:
        """Get valid values for a manifest attribute based on the corresponding
        values of node's properties in JSONSchema

        Args:
            prop: node properties - jsonschema dictionary

        Returns:
            List of valid values
        """

        if "enum" in prop:
            return prop["enum"]
        elif "items" in prop:
            return prop["items"]["enum"]
        else:
            return []

    def _get_json_schema(self, json_schema_filepath: str) -> Dict:
        """Open json schema as a dictionary.
        Args:
            json_schema_filepath(str): path to json schema file

        Returns:
            Dictionary, containing portions of the json schema
        """
        if not json_schema_filepath:
            # if no json schema is provided; there must be
            # schema explorer defined for schema.org schema
            # o.w. this will throw an error
            # TODO: catch error
            json_schema = self.sg.get_json_schema_requirements(self.root, self.title)
        else:
            with open(json_schema_filepath) as jsonfile:
                json_schema = json.load(jsonfile)
        return json_schema

    def _get_required_fields_per_manifest(
        self, primary_key: str, foreign_keys: list, json_schema: dict, prop_label_to_display_name: dict
    ) -> list[list]:
        """For a RDB root manifest, create a manifest per connected
        foreign key table, that contains only attributes for the root table
        and the foreign key. In this step gather the attributes/fields per manifest.

        Args:
            primary_key(str): primary key linked to the root node
            foreign_keys(list): foreign keys linked to the root node
            json_schema(dict): representing a handful of values
                representing the data model, including: '$schema', '$id', 'title',
                'type', 'properties', 'required'
            prop_label_to_display_name(dict):
                {property_label: display_name} for all property keys in
                the json schema.
        Returns:
            req_fields_per_manifest(list[list]):
                for each RDB root manifest that is generated, all the attributes/fields/cols
                that it should contain.
        """
        # Gather just the attributes that we want per manifest
        root_manifest_base_attributes = [
            *self.rdb.tables["Resource"]["attributes"].keys()
        ]
        root_manifest_base_attributes.append(primary_key)
        # Determine if component should be added to the manifest
        if "Component" in json_schema["required"]:
            add_component = True
        req_fields_per_manifest = []
        manifest_attributes = []
        # Each of the foreign keys represents a separate table that will
        # be linked to the root.
        for key in foreign_keys:
            manifest_attributes = root_manifest_base_attributes.copy()
            manifest_attributes.append(key)
            # Convert naming to property labels.
            manifest_attributes = [
                prop_label_to_display_name[attr] for attr in manifest_attributes
            ]
            if add_component:
                manifest_attributes.append("Component")
            req_fields_per_manifest.append(manifest_attributes)

        return req_fields_per_manifest

    def _get_required_metadata_fields(self, json_schema, fields):
        """For the root node gather dependency requirements (all attributes linked to this node)
        and corresponding allowed values constraints (i.e. valid values).

        Args:
            json_schema(dict): representing a handful of values
                representing the data model, including: '$schema', '$id', 'title',
                'type', 'properties', 'required'
            fields(list[str]): fields/attributes to search
        Returns:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
        """
        required_metadata_fields = {}
        for req in fields:
            required_metadata_fields[
                req
            ] = self._get_valid_values_from_jsonschema_property(
                json_schema["properties"][req]
            )
            # the following line may not be needed
            json_schema["properties"][req]["enum"] = required_metadata_fields[req]
        return required_metadata_fields

    def _gather_dependency_requirements(self, json_schema, required_metadata_fields):
        """Gathering dependency requirements and allowed value constraints
            for conditional dependencies, if any
        TODO:
            Refactor
        Args:
            json_schema(dict): representing a handful of values
                representing the data model, including: '$schema', '$id', 'title',
                'type', 'properties', 'required'
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
        Returns:
            required_metadata_fields(dict):
                updates dictionary to additional attributes, if applicable,
                as keys with their corresponding valid values.
        """

        if "allOf" in json_schema:
            for conditional_reqs in json_schema["allOf"]:
                if "required" in conditional_reqs["if"]:
                    for req in conditional_reqs["if"]["required"]:
                        if req in conditional_reqs["if"]["properties"]:
                            if not req in required_metadata_fields:
                                if req in json_schema["properties"]:
                                    required_metadata_fields[
                                        req
                                    ] = self._get_valid_values_from_jsonschema_property(
                                        json_schema["properties"][req]
                                    )
                                else:
                                    required_metadata_fields[
                                        req
                                    ] = self._get_valid_values_from_jsonschema_property(
                                        conditional_reqs["if"]["properties"][req]
                                    )

                    for req in conditional_reqs["then"]["required"]:
                        if not req in required_metadata_fields:
                            if req in json_schema["properties"]:
                                required_metadata_fields[
                                    req
                                ] = self._get_valid_values_from_jsonschema_property(
                                    json_schema["properties"][req]
                                )

        return required_metadata_fields

    def _add_root_to_component(self, required_metadata_fields):
        """If 'Component' is in the column set, add root node as a
        metadata component entry in the first row of that column.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
        Return:
            updates self.additional_metadata if appropriate to
            contain {'Component': [self.root]}
        """

        if "Component" in required_metadata_fields.keys():
            # check if additional metadata has actually been instantiated in the
            # constructor (it's optional) if not, instantiate it
            if not self.additional_metadata:
                self.additional_metadata = {}
            self.additional_metadata["Component"] = [self.root]
        return

    def _get_additional_metadata(self, required_metadata_fields: dict) -> dict:
        """Add additional metadata as entries to columns.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
        Return:
            required_metadata_fields(dict): Updated required_metadata_fields to include additional metadata
                that is to be added to specified columns
        """
        self._add_root_to_component(required_metadata_fields)
        # if additional metadata is provided append columns (if those do not exist already)
        if self.additional_metadata:
            for column in self.additional_metadata.keys():
                if not column in required_metadata_fields:
                    required_metadata_fields[column] = []
        return required_metadata_fields

    def _get_column_range_and_order(self, required_metadata_fields):
        """Find the alphabetical range of columns and sort them.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
        Returns:
            end_col_letter (chr): google sheet letter representation of a column index integer
            ordered_metadata_fields(List[list]): Contining all the attributes to
            add as columns, ordered
        """
        # determining columns range
        end_col = len(required_metadata_fields.keys())
        end_col_letter = self._column_to_letter(end_col)

        # order columns header (since they are generated based on a json schema, which is a dict)
        ordered_metadata_fields = [list(required_metadata_fields.keys())]
        ordered_metadata_fields[0] = self.sort_manifest_fields(
            ordered_metadata_fields[0]
        )
        return end_col_letter, ordered_metadata_fields

    def _gs_add_and_format_columns(self, required_metadata_fields, spreadsheet_id):
        """Add columns to the google sheet and format them.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
            spreadsheet_id(str): id of the google sheet
        Returns:
            respones: In return for a request, used by google sheet api to add columns
                to the sheet being generated.
            ordered_metadata_fields(List[list]): contining all the attributes to
            add as columns, ordered
        """

        end_col_letter, ordered_metadata_fields = self._get_column_range_and_order(
            required_metadata_fields
        )

        body = {"values": ordered_metadata_fields}

        range = "Sheet1!A1:" + str(end_col_letter) + "1"

        # adding columns
        self.sheet_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range, valueInputOption="RAW", body=body
        ).execute()

        # adding columns to 2nd sheet that can be used for storing data validation ranges (this avoids limitations on number of dropdown items in excel and openoffice)
        range = "Sheet2!A1:" + str(end_col_letter) + "1"
        self.sheet_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range, valueInputOption="RAW", body=body
        ).execute()

        # format column header row
        header_format_body = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {"startRowIndex": 0, "endRowIndex": 1},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": 224.0 / 255,
                                    "green": 224.0 / 255,
                                    "blue": 224.0 / 255,
                                },
                                "horizontalAlignment": "CENTER",
                                "textFormat": {
                                    "foregroundColor": {
                                        "red": 0.0 / 255,
                                        "green": 0.0 / 255,
                                        "blue": 0.0 / 255,
                                    },
                                    "fontSize": 8,
                                    "bold": True,
                                },
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {"gridProperties": {"frozenRowCount": 1}},
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "autoResizeDimensions": {
                        "dimensions": {"dimension": "COLUMNS", "startIndex": 0}
                    }
                },
            ]
        }

        response = (
            self.sheet_service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=header_format_body)
            .execute()
        )
        return response, ordered_metadata_fields

    def _gs_add_additional_metadata(
        self, required_metadata_fields, ordered_metadata_fields, spreadsheet_id
    ):
        """Adding additional metadata values, if needed. Add value-constraints
        from data model as dropdowns. Batch google API request to
        create metadata template.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
            ordered_metadata_fields(List[list]): contining all the attributes to
            add as columns, ordered
            spreadsheet_id(str): id for the google sheet
        Returns: Response(dict): For batch update to create metadata
        template.
        """
        data = []

        for i, req in enumerate(ordered_metadata_fields[0]):
            values = required_metadata_fields[req]

            if self.additional_metadata and req in self.additional_metadata:
                values = self.additional_metadata[req]
                target_col_letter = self._column_to_letter(i)

                range_vals = (
                    target_col_letter + "2:" + target_col_letter + str(len(values) + 1)
                )

                data.append(
                    {
                        "range": range_vals,
                        "majorDimension": "COLUMNS",
                        "values": [values],
                    }
                )

        batch_update_values_request_body = {
            # How the input data should be interpreted.
            "valueInputOption": "RAW",
            # The new values to apply to the spreadsheet.
            "data": data,
        }

        response = (
            self.sheet_service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id, body=batch_update_values_request_body
            )
            .execute()
        )
        return response

    def _request_row_format(self, i, req):
        """Adding description to headers, this is not executed if
        only JSON schema is defined. Also formatting required columns.
        Args:
            i (int): column index
            req (str): column name
        Returns:
            notes_body["requests"] (dict): with information on note
                to add to the column header. This notes body will be added to a request.
        """
        if self.sg.se:
            # get node definition
            note = self.sg.get_node_definition(req)

            notes_body = {
                "requests": [
                    {
                        "updateCells": {
                            "range": {
                                "startRowIndex": 0,
                                "endRowIndex": 1,
                                "startColumnIndex": i,
                                "endColumnIndex": i + 1,
                            },
                            "rows": [{"values": [{"note": note}]}],
                            "fields": "note",
                        }
                    }
                ]
            }

            return notes_body["requests"]
        else:
            return

    def _request_note_valid_values(self, i, req, validation_rules, valid_values):
        """If the node validation rule is 'list' and the node also contains
        valid values, add a note a note with instructions on adding a list of
        multiple values with the multi-select option in google sheets

        TODO: add validation and QC rules "compiler/generator" class elsewhere
            for now have the list logic here
        Args:
            i (int): column index
            req (str): column name
            validation_rules(list[str]): containing all relevant
                rules applied to node/column
            valid_values(list[str]): containing all valid values defined in the
                data model for this node/column

        Returns:
            notes_body["requests"] (dict): with information on note
                to add to the column header, about using multiselect.
                This notes body will be added to a request.
        """

        if "list" in validation_rules and valid_values:
            note = "From 'Selection options' menu above, go to 'Select multiple values', check all items that apply, and click 'Save selected values'"
            notes_body = {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "startRowIndex": 1,
                                "startColumnIndex": i,
                                "endColumnIndex": i + 1,
                            },
                            "cell": {"note": note},
                            "fields": "note",
                        }
                    }
                ]
            }

            return notes_body["requests"]
        else:
            return

    def _request_notes_comments(self, i, req, json_schema):
        """Update background colors so that columns that are required are highlighted
        Args:
            i (int): column index
            req (str): column name
            json_schema(dict): representing a handful of values
                representing the data model, including: '$schema', '$id', 'title',
                'type', 'properties', 'required'
        Returns:
            req_format_body["requests"] (dict): specifing the format updating for
                required attributes/columns
        """
        # check if attribute is required and set a corresponding color
        if req in json_schema["required"]:
            bg_color = CONFIG["style"]["google_manifest"].get(
                "req_bg_color",
                {
                    "red": 0.9215,
                    "green": 0.9725,
                    "blue": 0.9803,
                },
            )

            req_format_body = {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "startColumnIndex": i,
                                "endColumnIndex": i + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {"backgroundColor": bg_color}
                            },
                            "fields": "userEnteredFormat(backgroundColor)",
                        }
                    }
                ]
            }
            return req_format_body["requests"]
        else:
            return

    def _request_cell_borders(self):
        """Get cell border color specifications.
        Args: None
        Returns:
            Dict, containing cell border design specifications.
        """
        # setting cell borders
        cell_range = {
            "sheetId": 0,
            "startRowIndex": 0,
        }
        return self._get_cell_borders(cell_range)

    def _request_dropdown_or_multi(
        self, i, req_vals, spreadsheet_id, validation_rules, valid_values
    ):
        """Generating sheet api request to populate a dropdown or a multi selection UI
        Args:
            i (int): column index
            req_vals (list[dict]): dict for each valid value
                key: 'userEnteredValue'
                value: str, valid value
            spreadsheet_id (str): id for the google sheet
            validation_rules(list[str]): containing all relevant
                rules applied to node/column
            valid_values(list[str]): containing all valid values defined in the
                data model for this node/column
        Returns:
            validation_body: dict
        """
        if len(req_vals) > 0 and not "list" in validation_rules:
            # if more than 0 values in dropdown use ONE_OF_RANGE type of validation
            # since excel and openoffice
            # do not support other kinds of data validation for
            # larger number of items (even if individual items are not that many
            # excel has a total number of characters limit per dropdown...)
            validation_body = self._get_column_data_validation_values(
                spreadsheet_id, req_vals, i, validation_type="ONE_OF_RANGE"
            )
        elif "list" in validation_rules and valid_values:
            # if list is in validation rule attempt to create a multi-value
            # selection UI, which requires explicit valid values range in
            # the spreadsheet
            validation_body = self._get_column_data_validation_values(
                spreadsheet_id,
                req_vals,
                i,
                custom_ui=False,
                input_message="",
                validation_type="ONE_OF_RANGE",
            )
        else:
            validation_body = self._get_column_data_validation_values(
                spreadsheet_id, req_vals, i
            )
        return validation_body["requests"]

    def _dependency_formatting(
        self, i, req_val, ordered_metadata_fields, val_dependencies
    ):
        """If there are additional attribute dependencies find the corresponding
        fields that need to be filled in and construct conditional formatting rules
        indicating the dependencies need to be filled in.

        set target ranges for this rule
        i.e. dependency attribute columns that will be formatted

        Args:
            i (int): column index
            req_val (str): node name
            ordered_metadata_fields(List[list]): contining all the attributes to
            add as columns, ordered
            val_depenencies (list[str]): dependencies
        Returns:
            conditional_format_rule (dict):
                specifies gs conditional formatting
        """

        # find dependency column indexes
        # note that dependencies values must be in index
        # TODO: catch value error that shouldn't happen
        column_idxs = [
            ordered_metadata_fields[0].index(val_dep) for val_dep in val_dependencies
        ]

        # construct ranges based on dependency column indexes
        rule_ranges = self._columns_to_sheet_ranges(column_idxs)
        # go over valid value dependencies
        for j, val_dep in enumerate(val_dependencies):
            is_required = False

            if self.sg.is_node_required(val_dep):
                is_required = True
            else:
                is_required = False

            # construct formatting rule
            formatting_rule = self._column_to_cond_format_eq_rule(
                i, req_val, required=is_required
            )

            # construct conditional format rule
            conditional_format_rule = {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": rule_ranges[j],
                        "booleanRule": formatting_rule,
                    },
                    "index": 0,
                }
            }
        return conditional_format_rule

    def _request_dependency_formatting(
        self, i, req_vals, ordered_metadata_fields, requests_body
    ):
        """Gather all the formattng for node dependencies.
        Args:
            i (int): column index
            req_val (str): node name
            ordered_metadata_fields(List[list]): contining all the attributes to
            add as columns, ordered
            requests_body(dict):
                containing all the update requests to add to the gs
        Return:
            dependency_formatting_body (dict): specifiying the conditional
            formatting rules to apply
        """
        for req_val in req_vals:
            # get this required/valid value's node label in schema, based on display name (i.e. shown to the user in a dropdown to fill in)
            req_val = req_val["userEnteredValue"]

            req_val_node_label = self.sg.get_node_label(req_val)
            if not req_val_node_label:
                # if this node is not in the graph
                # continue - there are no dependencies for it
                continue

            # check if this required/valid value has additional dependency attributes
            val_dependencies = self.sg.get_node_dependencies(
                req_val_node_label, schema_ordered=False
            )

            # prepare request calls
            dependency_formatting_body = {"requests": []}

            # set conditiaon formatting for dependencies.
            if val_dependencies:
                conditional_format_rule = self._dependency_formatting(
                    i, req_val, ordered_metadata_fields, val_dependencies
                )
                dependency_formatting_body["requests"].append(conditional_format_rule)
        return dependency_formatting_body

    def _create_requests_body(
        self,
        required_metadata_fields,
        ordered_metadata_fields,
        json_schema,
        spreadsheet_id,
    ):
        """Create and store all formatting changes for the google sheet to
        execute at once.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
            ordered_metadata_fields: List[list], contining all the attributes to
                add as columns, ordered
            json_schema: dict, representing a handful of values
                representing the data model, including: '$schema', '$id', 'title',
                'type', 'properties', 'required'
            spreadsheet_id: str, of the id for the google sheet
        Return:
            requests_body(dict):
                containing all the update requests to add to the gs
        """
        # store all requests to execute at once
        requests_body = {}
        requests_body["requests"] = []
        for i, req in enumerate(ordered_metadata_fields[0]):
            # Gather validation rules and valid values for attribute
            validation_rules = self.sg.get_node_validation_rules(req)
            valid_values = self._get_valid_values_from_jsonschema_property(
                json_schema["properties"][req]
            )

            # Set row formatting
            get_row_formatting = self._request_row_format(i, req)
            if get_row_formatting:
                requests_body["requests"].append(get_row_formatting)

            # Add notes to headers to provide descriptions of the attribute
            header_notes = self._request_notes_comments(i, req, json_schema)
            if header_notes:
                requests_body["requests"].append(header_notes)
            # Add note on how to use multi-select, when appropriate
            note_vv = self._request_note_valid_values(
                i, req, validation_rules, valid_values
            )
            if note_vv:
                requests_body["requests"].append(note_vv)

            # Adding value-constraints, if any
            values = required_metadata_fields[req]
            req_vals = [{"userEnteredValue": value} for value in values if value]
            if not req_vals:
                continue

            # Create dropdown or multi-select options, where called for
            create_dropdown = self._request_dropdown_or_multi(
                i, req_vals, spreadsheet_id, validation_rules, valid_values
            )
            if create_dropdown:
                requests_body["requests"].append(create_dropdown)

            if not self.rdb_root:
                # generate a conditional format rule for each required value (i.e. valid value)
                # for this field (i.e. if this field is set to a valid value that may require additional
                # fields to be filled in, these additional fields will be formatted in a custom style (e.g. red background)
                dependency_formatting_body = self._request_dependency_formatting(
                    i, req_vals, ordered_metadata_fields, requests_body
                )
                if dependency_formatting_body["requests"]:
                    requests_body["requests"].append(
                        dependency_formatting_body["requests"]
                    )
        # Set borders formatting
        borders_formatting = self._request_cell_borders()
        if borders_formatting:
            requests_body["requests"].append(borders_formatting)
        return requests_body

    def _create_empty_gs(self, required_metadata_fields, json_schema, spreadsheet_id):
        """Generate requests to add columns and format the google sheet.
        Args:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
            json_schema: dict, representing a handful of values
                representing the data model, including: '$schema', '$id', 'title',
                'type', 'properties', 'required'
            spreadsheet_id: str, of the id for the google sheet
        Returns:
            manifest_url (str): url of the google sheet manifest.
        """
        # adding columns to manifest sheet
        response, ordered_metadata_fields = self._gs_add_and_format_columns(
            required_metadata_fields, spreadsheet_id
        )

        # Add additional metadata
        response = self._gs_add_additional_metadata(
            required_metadata_fields, ordered_metadata_fields, spreadsheet_id
        )

        # Create requests body to add additional formatting to the sheets
        requests_body = self._create_requests_body(
            required_metadata_fields,
            ordered_metadata_fields,
            json_schema,
            spreadsheet_id,
        )

        # Execute requests
        execute_google_api_requests(
            self.sheet_service,
            requests_body,
            service_type="batch_update",
            spreadsheet_id=spreadsheet_id,
        )

        # Setting up spreadsheet permissions (setup so that anyone with the link can edit)
        self._set_permissions(spreadsheet_id)

        # generating spreadsheet URL
        manifest_url = "https://docs.google.com/spreadsheets/d/" + spreadsheet_id
        return manifest_url

    def _gather_all_fields(self, fields, json_schema):
        """Gather all the attributes/fields to include as columns in the manifest.
        Args:
            fields(list[str]): fields/attributes to search
        Returns:
            required_metadata_fields(dict):
                keys: of all the fields/attributes that need to be added
                    to the manifest
                values(list[str]): valid values
        """
        # Get required fields
        required_metadata_fields = self._get_required_metadata_fields(
            json_schema, fields
        )

        if not self.rdb_root:
            # Add additioal dependencies
            required_metadata_fields = self._gather_dependency_requirements(
                json_schema, required_metadata_fields
            )
        # Add additional metadata as entries to columns
        required_metadata_fields = self._get_additional_metadata(
            required_metadata_fields
        )
        return required_metadata_fields

    def _get_rdb_root_keys(self) -> list[str]:
        """Get all foreign keys related to the root manifest.
        Used only for relational databases.

        Args:
            None
        Return:
            primary_key(str): primary key for the root
            foreign_keys List, of foreign keys (str)
        """
        # get foreign keys based on db schema graph
        self.rdb = RDB(self.path_to_json_ld)

        primary_key = self.sg.se.get_property_label_from_display_name(self.max_authority +'_id')

        # Gather Foreign Key Ids so we know what values to keep per sheet.
        foreign_keys = self.rdb.get_table_foreign_keys(
            self.max_authority, table_prefix=False
        )
        foreign_keys.extend(self.rdb.get_additional_foreign_keys(self.max_authority))
        
        return primary_key, foreign_keys

    def get_empty_manifest(self, json_schema_filepath=None):
        """Create an empty manifest using specifications from the
        json schema.
        Args:
            json_schema_filepath (str): path to json schema file
        Returns:
            manifest_url (str): url of the google sheet manifest.
        """

        # Check if creating an empty manifest for the root node of
        # a relational database. If so, construct them a different way.
        if self.rdb_root:
            manifest_urls = self.generate_empty_root_manifests()
            return manifest_urls

        spreadsheet_id = self._create_empty_manifest_spreadsheet(self.title)
        json_schema = self._get_json_schema(json_schema_filepath)

        required_metadata_fields = self._gather_all_fields(
            json_schema["properties"].keys(), json_schema
        )

        manifest_url = self._create_empty_gs(
            required_metadata_fields, json_schema, spreadsheet_id
        )
        return manifest_url

    def generate_empty_root_manifests(self, json_schema_filepath=None):
        """In the case of a RDB, when using FK/PKs and having nested dependecies
        want to only pull relevant columns for the root directory.
        Make a manifest for each table that is connected through a foreign key,
        user feedback states this is an easier way to work with the data. Doing it
        this way will also get through the issue of having many 'required'
        foreign keys per row, which cannot possibly be supplied.

        Keeping separate from get_empty_manifest since it would
        require too many if statements to be easily readable.

        Args:
            json_schema_filepath (str): path to json schema file
        Returns:
            manifest_urls (list[str]):
                list of individual manifest_url's.
                Each of whom link to a gs manifest.
        """
        json_schema = self._get_json_schema(json_schema_filepath)
        primary_key, foreign_keys = self._get_rdb_root_keys()
        spreadsheet_ids = [
            self._create_empty_manifest_spreadsheet(self.title) for i in foreign_keys
        ]
        prop_label_to_display_name = self.sg.se.property_label_to_display_dict(
            json_schema["properties"].keys()
        )
        # Determine the fields/attributes to include per manifest
        req_fields_per_manifest = self._get_required_fields_per_manifest(primary_key,
            foreign_keys, json_schema, prop_label_to_display_name
        )
        # Gather all data for generating each manifest
        required_metadata_fields_per_manifest = []
        for fields in req_fields_per_manifest:
            required_metadata_fields = self._gather_all_fields(fields, json_schema)
            required_metadata_fields_per_manifest.append(required_metadata_fields)

        # Create each manifest
        manifest_urls = []
        for ind, required_metadata_fields in enumerate(
            required_metadata_fields_per_manifest
        ):
            manifest_urls.append(
                self._create_empty_gs(
                    required_metadata_fields, json_schema, spreadsheet_ids[ind]
                )
            )
        return manifest_urls

    def set_dataframe_by_url(
        self, manifest_url: str, manifest_df: pd.DataFrame
    ) -> ps.Spreadsheet:
        """Update Google Sheets using given pandas DataFrame.

        Args:
            manifest_url (str): Google Sheets URL.
            manifest_df (pd.DataFrame): Data frame to "upload".

        Returns:
            ps.Spreadsheet: A Google Sheet object.
        """

        # authorize pygsheets to read from the given URL
        gc = ps.authorize(custom_credentials=self.creds)

        # open google sheets and extract first sheet
        sh = gc.open_by_url(manifest_url)
        wb = sh[0]

        # Handle scenario when existing manifest does not match new
        #       manifest template due to changes in the data model:
        #
        # the sheet column header reflect the latest schema
        # the existing manifest column-set may be outdated
        # ensure that, if missing, attributes from the latest schema are added to the
        # column-set of the existing manifest so that the user can modify their data if needed
        # to comply with the latest schema

        # get headers from existing manifest and sheet
        wb_header = wb.get_row(1)
        manifest_df_header = manifest_df.columns

        # find missing columns in existing manifest
        new_columns = set(wb_header) - set(manifest_df_header)

        # find missing columns present in existing manifest but missing in latest schema
        out_of_schema_columns = set(manifest_df_header) - set(wb_header)

        # update existing manifest w/ missing columns, if any
        if new_columns:
            manifest_df = manifest_df.assign(
                **dict(zip(new_columns, len(new_columns) * [""]))
            )

        # sort columns in the updated manifest:
        # match latest schema order
        # move obsolete columns at the end
        manifest_df = manifest_df[self.sort_manifest_fields(manifest_df.columns)]
        manifest_df = manifest_df[
            [c for c in manifest_df if c not in out_of_schema_columns]
            + list(out_of_schema_columns)
        ]

        # The following line sets `valueInputOption = "RAW"` in pygsheets
        sh.default_parse = False

        # update spreadsheet with given manifest starting at top-left cell
        wb.set_dataframe(manifest_df, (1, 1))

        # update validation rules (i.e. no validation rules) for out of schema columns, if any
        # TODO: similarly clear formatting for out of schema columns, if any
        if len(out_of_schema_columns) > 0:
            start_col = self._column_to_letter(
                len(wb_header)
            )  # find start of out of schema columns
            end_col = self._column_to_letter(
                len(manifest_df.columns) + 1
            )  # find end of out of schema columns
            wb.set_data_validation(start=start_col, end=end_col, condition_type=None)

        # set permissions so that anyone with the link can edit
        sh.share("", role="writer", type="anyone")

        return sh

    def get_dataframe_by_url(self, manifest_url: str) -> pd.DataFrame:
        """Retrieve pandas DataFrame from table in Google Sheets.

        Args:
            manifest_url (str): Google Sheets URL.

        Return:
            pd.DataFrame: Data frame corresponding to table in given URL.
        """

        # authorize pygsheets to read from the given URL
        gc = ps.authorize(custom_credentials=self.creds)

        # open google sheets and extract first sheet
        sh = gc.open_by_url(manifest_url)
        wb = sh[0]

        # get column headers and read it into a dataframe
        manifest_df = wb.get_as_df(hasHeader=True)

        # An empty column is sometimes included
        if "" in manifest_df:
            manifest_df.drop(columns=[""], inplace=True)

        return manifest_df

    def map_annotation_names_to_display_names(
        self, annotations: pd.DataFrame
    ) -> pd.DataFrame:
        """Update columns names to use display names for consistency.

        Args:
            annotations (pd.DataFrame): Annotations table.

        Returns:
            pd.DataFrame: Annotations table with updated column headers.
        """
        # Get list of attribute nodes from data model
        model_nodes = self.sg.se.get_nx_schema().nodes

        # Subset annotations to those appearing as a label in the model
        labels = filter(lambda x: x in model_nodes, annotations.columns)

        # Generate a dictionary mapping labels to display names
        label_map = {l: model_nodes[l]["displayName"] for l in labels}

        # Use the above dictionary to rename columns in question
        return annotations.rename(columns=label_map)

    def get_manifest_with_annotations(
        self, annotations: pd.DataFrame
    ) -> Tuple[ps.Spreadsheet, pd.DataFrame]:
        """Generate manifest, optionally with annotations (if requested).

        Args:
            annotations (pd.DataFrame): Annotations table (can be empty).

        Returns:
            Tuple[ps.Spreadsheet, pd.DataFrame]: Both the Google Sheet
            URL and the corresponding data frame is returned.
        """

        # Map annotation labels to display names to match manifest columns
        annotations = self.map_annotation_names_to_display_names(annotations)

        # Convert annotations table into dictionary, but maintain order
        annotations_dict_raw = annotations.to_dict(into=OrderedDict)
        annotations_dict = OrderedDict(
            (k, list(v.values())) for k, v in annotations_dict_raw.items()
        )

        # Needs to happen before get_empty_manifest() gets called
        self.additional_metadata = annotations_dict

        # Generate empty manifest using `additional_metadata`
        manifest_url = self.get_empty_manifest()

        manifest_df = self.get_dataframe_by_url(manifest_url)

        # Annotations clashing with manifest attributes are skipped
        # during empty manifest generation. For more info, search
        # for `additional_metadata` in `self.get_empty_manifest`.
        # Hence, the shared columns need to be updated separately.
        if self.is_file_based and self.use_annotations:
            # This approach assumes that `update_df` returns
            # a data frame whose columns are in the same order
            manifest_df = update_df(manifest_df, annotations)
            manifest_sh = self.set_dataframe_by_url(manifest_url, manifest_df)
            manifest_url = manifest_sh.url

        return manifest_url, manifest_df

    def get_manifest(
        self, dataset_id: str = None, sheet_url: bool = None, json_schema: str = None
    ):
        """Gets manifest for a given dataset on Synapse.

        Args:
            dataset_id: Synapse ID of the "dataset" entity on Synapse (for a given center/project).
            sheet_url: Determines if googlesheet URL or pandas dataframe should be returned.

        Returns:
            Googlesheet URL (if sheet_url is True), or pandas dataframe (if sheet_url is False).
        """

        # Handle case when no dataset ID is provided
        if not dataset_id:
            return self.get_empty_manifest(json_schema_filepath=json_schema)

        # Otherwise, create manifest using the given dataset
        syn_store = SynapseStorage()

        # Get manifest file associated with given dataset (if applicable)
        syn_id_and_path = syn_store.getDatasetManifest(datasetId=dataset_id)

        # Populate empty template with existing manifest
        if syn_id_and_path:

            # TODO: Update or remove the warning in self.__init__() if
            # you change the behavior here based on self.use_annotations

            # get synapse ID manifest associated with dataset
            manifest_data = syn_store.getDatasetManifest(
                datasetId=dataset_id, downloadFile=True
            )

            # If the sheet URL isn't requested, simply return a pandas DataFrame
            if not sheet_url:
                return pd.read_csv(manifest_data.path)

            # get URL of an empty manifest file created based on schema component
            empty_manifest_url = self.get_empty_manifest()

            # populate empty manifest with content from downloaded/existing manifest
            pop_manifest_url = self.populate_manifest_spreadsheet(
                manifest_data.path, empty_manifest_url
            )

            return pop_manifest_url

        # Generate empty template and optionally fill in with annotations
        else:

            # Using getDatasetAnnotations() to retrieve file names and subset
            # entities to files and folders (ignoring tables/views)
            annotations = pd.DataFrame()
            if self.is_file_based:
                annotations = syn_store.getDatasetAnnotations(dataset_id)

            # Subset columns if no interested in user-defined annotations
            if self.is_file_based and not self.use_annotations:
                annotations = annotations[["Filename", "eTag", "entityId"]]

            # Update `additional_metadata` and generate manifest
            manifest_url, manifest_df = self.get_manifest_with_annotations(annotations)

            if sheet_url:
                return manifest_url
            else:
                return manifest_df

    def populate_manifest_spreadsheet(self, existing_manifest_path, empty_manifest_url):
        """Creates a google sheet manifest based on existing manifest.

        Args:
            existing_manifest_path: the location of the manifest containing metadata presently stored
            empty_manifest_url: the path to a manifest template to be prepopulated with existing's manifest metadata
        """

        # read existing manifest
        manifest = pd.read_csv(existing_manifest_path).fillna("")

        manifest_sh = self.set_dataframe_by_url(empty_manifest_url, manifest)

        return manifest_sh.url

    def sort_manifest_fields(self, manifest_fields, order="schema"):
        # order manifest fields alphabetically (base order)
        manifest_fields = sorted(manifest_fields)

        if order == "alphabetical":
            # if the order is alphabetical ensure that filename is first, if present
            if "Filename" in manifest_fields:
                manifest_fields.remove("Filename")
                manifest_fields.insert(0, "Filename")

        # order manifest fields based on data-model schema
        if order == "schema":
            if self.sg and self.root:
                # get display names of dependencies
                dependencies_display_names = self.sg.get_node_dependencies(self.root)

                # reorder manifest fields so that root dependencies are first and follow schema order
                manifest_fields = sorted(
                    manifest_fields,
                    key=lambda x: dependencies_display_names.index(x)
                    if x in dependencies_display_names
                    else len(manifest_fields) - 1,
                )
            else:
                raise ValueError(
                    f"Provide valid data model path and valid component from data model."
                )

        # always have entityId as last columnn, if present
        if "entityId" in manifest_fields:
            manifest_fields.remove("entityId")
            manifest_fields.append("entityId")

        return manifest_fields
