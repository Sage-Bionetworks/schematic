import os
import logging
from typing import Dict, List, Tuple
from collections import OrderedDict
from tempfile import NamedTemporaryFile

import pandas as pd
import pygsheets as ps
import json

from schematic.schemas.generator import SchemaGenerator
from schematic.utils.google_api_utils import (
    build_credentials,
    execute_google_api_requests,
    build_service_account_creds,
)
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

        # SchemaGenerator() object
        self.sg = SchemaGenerator(path_to_json_ld)

        # additional metadata to add to manifest
        self.additional_metadata = additional_metadata

        # Determine whether current data type is file-based
        is_file_based = False
        if self.root:
            is_file_based = "Filename" in self.sg.get_node_dependencies(self.root)
        self.is_file_based = is_file_based

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

        strict = CONFIG["style"]["google_manifest"].get(
            "strict_validation",
            True)

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

    def get_empty_manifest(self, json_schema_filepath=None):
        # TODO: Refactor get_manifest method
        # - abstract function for requirements gathering
        # - abstract google sheet API requests as functions
        # --- specifying row format
        # --- setting valid values in dropdowns for columns/cells
        # --- setting notes/comments to cells

        spreadsheet_id = self._create_empty_manifest_spreadsheet(self.title)

        if not json_schema_filepath:
            # if no json schema is provided; there must be
            # schema explorer defined for schema.org schema
            # o.w. this will throw an error
            # TODO: catch error
            json_schema = self.sg.get_json_schema_requirements(self.root, self.title)
        else:
            with open(json_schema_filepath) as jsonfile:
                json_schema = json.load(jsonfile)

        required_metadata_fields = {}

        # gathering dependency requirements and corresponding allowed values constraints (i.e. valid values) for root node
        for req in json_schema["properties"].keys():
            required_metadata_fields[
                req
            ] = self._get_valid_values_from_jsonschema_property(
                json_schema["properties"][req]
            )
            # the following line may not be needed
            json_schema["properties"][req]["enum"] = required_metadata_fields[req]

        # gathering dependency requirements and allowed value constraints for conditional dependencies if any
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

        # if additional metadata is provided append columns (if those do not exist already)
        if self.additional_metadata:
            for column in self.additional_metadata.keys():
                if not column in required_metadata_fields:
                    required_metadata_fields[column] = []

        # if 'component' is in column set (see your input jsonld schema for definition of 'component', if the 'component' attribute is present), add the root node as an additional metadata component entry
        if "Component" in required_metadata_fields.keys():
            # check if additional metadata has actually been instantiated in the constructor (it's optional)
            # if not, instantiate it
            if not self.additional_metadata:
                self.additional_metadata = {}

            self.additional_metadata["Component"] = [self.root]

        # adding columns to manifest sheet
        end_col = len(required_metadata_fields.keys())
        end_col_letter = self._column_to_letter(end_col)

        # order columns header (since they are generated based on a json schema, which is a dict)
        ordered_metadata_fields = [list(required_metadata_fields.keys())]

        ordered_metadata_fields[0] = self.sort_manifest_fields(
            ordered_metadata_fields[0]
        )

        body = {"values": ordered_metadata_fields}

        # determining columns range
        end_col = len(required_metadata_fields.keys())
        end_col_letter = self._column_to_letter(end_col)

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

        # adding additional metadata values if needed
        # adding value-constraints from data model as dropdowns

        # fix for issue #410
        # batch google API request to create metadata template
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

        # end of fix for issue #410

        # store all requests to execute at once
        requests_body = {}
        requests_body["requests"] = []
        for i, req in enumerate(ordered_metadata_fields[0]):
            values = required_metadata_fields[req]

            # adding description to headers
            # this is not executed if only JSON schema is defined
            # TODO: abstract better and document

            # also formatting required columns
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

                requests_body["requests"].append(notes_body["requests"])

            # get node validation rules if any
            validation_rules = self.sg.get_node_validation_rules(req)

            # if 'list' in validation rules add a note with instructions on
            # adding a list of multiple values
            # TODO: add validation and QC rules "compiler/generator" class elsewhere
            # for now have the list logic here
            if "list" in validation_rules:
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

                requests_body["requests"].append(notes_body["requests"])

            # update background colors so that columns that are required are highlighted
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

                requests_body["requests"].append(req_format_body["requests"])

            # adding value-constraints if any
            req_vals = [{"userEnteredValue": value} for value in values if value]

            if not req_vals:
                continue

            # generating sheet api request to populate a dropdown or a multi selection UI
            if len(req_vals) > 0 and not "list" in validation_rules:
                # if more than 0 values in dropdown use ONE_OF_RANGE type of validation since excel and openoffice
                # do not support other kinds of data validation for larger number of items (even if individual items are not that many
                # excel has a total number of characters limit per dropdown...)
                validation_body = self._get_column_data_validation_values(
                    spreadsheet_id, req_vals, i, validation_type="ONE_OF_RANGE"
                )

            elif "list" in validation_rules:
                # if list is in validation rule attempt to create a multi-value
                # selection UI, which requires explicit valid values range in
                # the spreadsheet
                validation_body = self._get_column_data_validation_values(
                    spreadsheet_id,
                    req_vals,
                    i,
                    strict=False,
                    custom_ui=False,
                    input_message="",
                    validation_type="ONE_OF_RANGE",
                )

            else:
                validation_body = self._get_column_data_validation_values(
                    spreadsheet_id, req_vals, i
                )

            requests_body["requests"].append(validation_body["requests"])

            # generate a conditional format rule for each required value (i.e. valid value)
            # for this field (i.e. if this field is set to a valid value that may require additional
            # fields to be filled in, these additional fields will be formatted in a custom style (e.g. red background)
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

                if val_dependencies:
                    # if there are additional attribute dependencies find the corresponding
                    # fields that need to be filled in and construct conditional formatting rules
                    # indicating the dependencies need to be filled in

                    # set target ranges for this rule
                    # i.e. dependency attribute columns that will be formatted

                    # find dependency column indexes
                    # note that dependencies values must be in index
                    # TODO: catch value error that shouldn't happen
                    column_idxs = [
                        ordered_metadata_fields[0].index(val_dep)
                        for val_dep in val_dependencies
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
                        dependency_formatting_body["requests"].append(
                            conditional_format_rule
                        )

                # check if dependency formatting rules have been added and update sheet if so
                if dependency_formatting_body["requests"]:
                    requests_body["requests"].append(
                        dependency_formatting_body["requests"]
                    )

        # setting cell borders
        cell_range = {
            "sheetId": 0,
            "startRowIndex": 0,
        }
        requests_body["requests"].append(self._get_cell_borders(cell_range))

        execute_google_api_requests(
            self.sheet_service,
            requests_body,
            service_type="batch_update",
            spreadsheet_id=spreadsheet_id,
        )

        # setting up spreadsheet permissions (setup so that anyone with the link can edit)
        self._set_permissions(spreadsheet_id)

        # generating spreadsheet URL
        manifest_url = "https://docs.google.com/spreadsheets/d/" + spreadsheet_id

        # print("========================================================================================================")
        # print("Manifest successfully generated from schema!")
        # print("URL: " + manifest_url)
        # print("========================================================================================================")

        return manifest_url

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

        # The following line sets `valueInputOption = "RAW"` in pygsheets
        sh.default_parse = False

        # update spreadsheet with given manifest starting at top-left cell
        wb.set_dataframe(manifest_df, (1, 1))

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

        # sort manifest columns
        manifest_fields = manifest.columns.tolist()
        manifest_fields = self.sort_manifest_fields(manifest_fields)
        manifest = manifest[manifest_fields]

        # TODO: Handle scenario when existing manifest does not match new
        #       manifest template due to changes in the data model
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

        # order manifest fields based on schema (schema.org)
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
                            
