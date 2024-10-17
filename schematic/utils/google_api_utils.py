"""Google API utils"""

# pylint: disable=logging-fstring-interpolation

import json
import logging
import os
from typing import Any, Callable, TypedDict, Union, no_type_check

import pandas as pd
from google.oauth2 import service_account  # type: ignore
from googleapiclient.discovery import Resource, build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
)

from schematic.configuration.configuration import CONFIG

logger = logging.getLogger(__name__)


# If modifying these scopes, delete the file token.pickle.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleServiceAcountCreds(TypedDict):
    "Service account credentials for Google sheets"
    sheet_service: Resource
    drive_service: Resource
    creds: service_account.Credentials


def build_service_account_creds() -> GoogleServiceAcountCreds:
    """Build Google service account credentials

    Returns:
        GoogleServiceAcountCreds: The credentials
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
    sheet_service: Resource = build("sheets", "v4", credentials=credentials)
    # get a Google Drive API service
    drive_service: Resource = build("drive", "v3", credentials=credentials)

    creds: GoogleServiceAcountCreds = {
        "sheet_service": sheet_service,
        "drive_service": drive_service,
        "creds": credentials,
    }
    return creds


@no_type_check
def execute_google_api_requests(service, requests_body, **kwargs) -> Any:
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
        response = google_api_execute_wrapper(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=kwargs["spreadsheet_id"], body=requests_body)
            .execute
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
    # Pylint seems to have trouble with the google api classes, recognizing their methods
    data = google_api_execute_wrapper(
        drive_service.files()  # pylint: disable=no-member
        .export(fileId=spreadsheet_id, mimeType=mime_type)
        .execute
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


@retry(
    stop=stop_after_attempt(5),
    wait=wait_chain(
        *[wait_fixed(1) for i in range(2)]
        + [wait_fixed(2) for i in range(2)]
        + [wait_fixed(5)]
    ),
    retry=retry_if_exception_type(HttpError),
    reraise=True,
)
def google_api_execute_wrapper(api_function_to_call: Callable[[], Any]) -> Any:
    """Retry wrapper for Google API calls, with a backoff strategy.

    Args:
        api_function_to_call (Callable[[], Any]): The function to call

    Returns:
        Any: The result of the API call
    """
    return api_function_to_call()
