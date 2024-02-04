"""Google API utils"""

import os
import logging
import json
from typing import Any, Union, Optional

import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account

from schematic.configuration.configuration import CONFIG
from schematic.store.synapse import SynapseStorage

# pylint: disable=logging-fstring-interpolation

logger = logging.getLogger(__name__)


# If modifying these scopes, delete the file token.pickle.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# This function doesn't appear to be used or tested anywhere in schematic.
# TO DO: replace by pygsheets calls?
def build_credentials() -> dict[str, Any]:  # pylint: disable=missing-function-docstring
    creds = generate_token()  # pylint: disable=undefined-variable

    # get a Google Sheet API service
    sheet_service = build("sheets", "v4", credentials=creds)
    # get a Google Drive API service
    drive_service = build("drive", "v3", credentials=creds)

    return {
        "sheet_service": sheet_service,
        "drive_service": drive_service,
        "creds": creds,
    }


def build_service_account_creds() -> dict[str, Any]:
    """Build Google service account credentials

    Returns:
        dict[str, Any]: The credentials
    """
    if "SERVICE_ACCOUNT_CREDS" in os.environ:
        dict_creds = json.loads(os.environ["SERVICE_ACCOUNT_CREDS"])
        credentials = service_account.Credentials.from_service_account_info(
            dict_creds, scopes=SCOPES
        )

    # for AWS deployment
    elif "SECRETS_MANAGER_SECRETS" in os.environ:
        all_secrets_dict = json.loads(os.environ["SECRETS_MANAGER_SECRETS"])
        dict_creds = json.loads(all_secrets_dict["SERVICE_ACCOUNT_CREDS"])
        credentials = service_account.Credentials.from_service_account_info(
            dict_creds, scopes=SCOPES
        )
    else:
        credentials = service_account.Credentials.from_service_account_file(
            CONFIG.service_account_credentials_path, scopes=SCOPES
        )

    # get a Google Sheet API service
    sheet_service = build("sheets", "v4", credentials=credentials)
    # get a Google Drive API service
    drive_service = build("drive", "v3", credentials=credentials)

    return {
        "sheet_service": sheet_service,
        "drive_service": drive_service,
        "creds": credentials,
    }


def download_creds_file() -> None:
    """Download google credentials file"""
    syn = SynapseStorage.login()

    # if file path of service_account does not exist
    # and if an environment variable related to service account is not found
    # regenerate service_account credentials
    if (
        not os.path.exists(CONFIG.service_account_credentials_path)
        and "SERVICE_ACCOUNT_CREDS" not in os.environ
    ):
        # synapse ID of the 'schematic_service_account_creds.json' file
        api_creds = CONFIG.service_account_credentials_synapse_id

        # Download in parent directory of SERVICE_ACCT_CREDS to
        # ensure same file system for os.rename()
        creds_dir = os.path.dirname(CONFIG.service_account_credentials_path)

        creds_file = syn.get(api_creds, downloadLocation=creds_dir)
        os.rename(creds_file.path, CONFIG.service_account_credentials_path)

        logger.info(
            "The credentials file has been downloaded "
            f"to '{CONFIG.service_account_credentials_path}'"
        )

    elif "SERVICE_ACCOUNT_CREDS" in os.environ:
        # remind users that "SERVICE_ACCOUNT_CREDS" as an environment variable is being used
        logger.info(
            "Using environment variable SERVICE_ACCOUNT_CREDS as the credential file."
        )


def execute_google_api_requests(service: Any, requests_body: Any, **kwargs) -> Any:
    """
    Execute google API requests batch; attempt to execute in parallel.

    Args:
        service (Any): google api service; for now assume google sheets service that is
          instantiated and authorized
        requests_body (Any): _description_
        kwargs: google API service parameters

    Returns:
        Any: google API response or None
    """
    if (
        "spreadsheet_id" in kwargs
        and "service_type" in kwargs
        and kwargs["service_type"] == "batch_update"
    ):
        # execute all requests
        response = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=kwargs["spreadsheet_id"], body=requests_body)
            .execute()
        )

        return response
    return None


def export_manifest_drive_service(
    manifest_url: str, file_path: str, mime_type: str
) -> None:
    """
    Export manifest by using google drive api. If export as an Excel spreadsheet,
      the exported spreadsheet would also include a hidden sheet
    result: Google sheet gets exported in desired format

    Args:
        manifest_url (str): google sheet manifest url
        file_path (str): file path of the exported manifest
        mime_type (str):  exporting mimetype
    """
    # initialize drive service
    services_creds = build_service_account_creds()
    drive_service = services_creds["drive_service"]

    # get spreadsheet id
    spreadsheet_id = manifest_url.split("/")[-1]

    # use google drive
    data = (
        drive_service.files()  # pylint: disable=no-member
        .export(fileId=spreadsheet_id, mimeType=mime_type)
        .execute()
    )

    # open file and write data
    with open(os.path.abspath(file_path), "wb") as fle:
        try:
            fle.write(data)
        except FileNotFoundError as not_found:
            logger.error(f"{not_found.filename} could not be found")


def export_manifest_csv(file_path: str, manifest: Union[pd.DataFrame, str]) -> None:
    """
    Export manifest as a CSV by using google drive api
    result: Google sheet gets exported as a CSV

    Args:
        file_path (str):  file path of the exported manifest
        manifest (Union[pd.DataFrame, str]): could be a dataframe or a manifest url
    """
    if isinstance(manifest, pd.DataFrame):
        manifest.to_csv(file_path, index=False)
    else:
        export_manifest_drive_service(manifest, file_path, mime_type="text/csv")


# This function doesn't appear to be used or tested
# pd.ExcelWriter is an ABC class which means it SHOULD NOT be instantiated
def export_manifest_excel(
    manifest: Union[pd.DataFrame, str], output_excel: Optional[str] = None
) -> None:
    """
    Export manifest as an Excel spreadsheet by using google sheet API.
    This approach could export hidden sheet
    Google sheet gets exported as an excel spreadsheet.
    If there's a hidden sheet, the hidden sheet also gets exported.

    Args:
        manifest (Union[pd.DataFrame, str]): could be a dataframe or a manifest url
        output_excel (Optional[str], optional): name of the exported manifest sheet.
          Defaults to None.
    """
    # pylint: disable=abstract-class-instantiated
    # pylint: disable=no-member

    # initialize drive service
    services_creds = build_service_account_creds()
    sheet_service = services_creds["sheet_service"]

    if isinstance(manifest, pd.DataFrame):
        manifest.to_excel(output_excel, index=False)
    else:
        # get spreadsheet id from url
        spreadsheet_id = manifest.split("/")[-1]

        # use google sheet api
        sheet_metadata = (
            sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        )
        sheets = sheet_metadata.get("sheets")

        # export to Excel
        writer = pd.ExcelWriter(output_excel)

        # export each sheet in manifest
        for sheet in sheets:
            dataset = (
                sheet_service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=sheet["properties"]["title"])
                .execute()
            )
            dataset_df = pd.DataFrame(dataset["values"])
            dataset_df.columns = dataset_df.iloc[0]
            dataset_df.drop(dataset_df.index[0], inplace=True)
            dataset_df.to_excel(
                writer, sheet_name=sheet["properties"]["title"], index=False
            )
        writer.save()
        writer.close()
