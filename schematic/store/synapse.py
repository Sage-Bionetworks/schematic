"""Synapse storage class"""

import asyncio
import atexit
import logging
import os
import re
import secrets
import shutil
import time
import uuid  # used to generate unique names for entities
from copy import deepcopy
from dataclasses import dataclass, field
from time import sleep

# allows specifying explicit variable types
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import numpy as np
import pandas as pd
import synapseclient
from opentelemetry import trace
from synapseclient import Annotations as OldAnnotations
from synapseclient import (
    Column,
    EntityViewSchema,
    EntityViewType,
    File,
    Folder,
    Schema,
    Synapse,
    Table,
    as_table_columns,
)
from synapseclient.annotations import _convert_to_annotations_list
from synapseclient.api import get_config_file, get_entity_id_bundle2
from synapseclient.core.constants.concrete_types import PROJECT_ENTITY
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseHTTPError,
    SynapseUnmetAccessRestrictions,
)
from synapseclient.models.annotations import Annotations
from synapseclient.table import CsvFileTable, Schema, build_table
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
)

from schematic.configuration.configuration import CONFIG
from schematic.exceptions import AccessCredentialsError
from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.store.base import BaseStorage
from schematic.store.database.synapse_database import SynapseDatabase
from schematic.store.synapse_tracker import SynapseEntityTracker
from schematic.utils.df_utils import (
    STR_NA_VALUES_FILTERED,
    col_in_dataframe,
    load_df,
    update_df,
)

# entity_type_mapping, get_dir_size, create_temp_folder, check_synapse_cache_size, and clear_synapse_cache functions are used for AWS deployment
# Please do not remove these import statements
from schematic.utils.general import (
    check_synapse_cache_size,
    clear_synapse_cache,
    create_temp_folder,
    entity_type_mapping,
    get_dir_size,
)
from schematic.utils.io_utils import cleanup_temporary_storage
from schematic.utils.schema_utils import get_class_label_from_display_name
from schematic.utils.validate_utils import comma_separated_list_regex, rule_in_rule_list

logger = logging.getLogger("Synapse storage")

tracer = trace.get_tracer("Schematic")


@dataclass
class ManifestDownload(object):
    """
    syn: an object of type synapseclient.
    manifest_id: id of a manifest
    synapse_entity_tracker: Tracker for a pull-through cache of Synapse entities
    """

    syn: synapseclient.Synapse
    manifest_id: str
    synapse_entity_tracker: SynapseEntityTracker = field(
        default_factory=SynapseEntityTracker
    )

    def _download_manifest_to_folder(self, use_temporary_folder: bool = True) -> File:
        """
        Try downloading a manifest to a specific folder (temporary or not). When the
        `use_temporary_folder` is set to True, the manifest will be downloaded to a
        temporary folder. This is useful for when the code is running as an API server
        where multiple requests are being made at the same time. This will prevent
        multiple requests from overwriting the same manifest file. When the
        `use_temporary_folder` is set to False, the manifest will be downloaded to the
        default manifest folder.

        Args:
            use_temporary_folder: boolean argument indicating if a temporary folder
                should be used to store the manifest file. This is useful when running
                this code as an API server where multiple requests could be made at the
                same time. This is set to False when the code is being used from the
                CLI. Defaults to True.

        Return:
            manifest_data: A Synapse file entity of the downloaded manifest
        """
        manifest_data = self.synapse_entity_tracker.get(
            synapse_id=self.manifest_id,
            syn=self.syn,
            download_file=False,
            retrieve_if_not_present=False,
        )
        current_span = trace.get_current_span()
        if (
            manifest_data
            and (file_handle := manifest_data.get("_file_handle", None))
            and current_span.is_recording()
        ):
            current_span.set_attribute(
                "schematic.manifest_size", file_handle.get("contentSize", 0)
            )

        if manifest_data and manifest_data.path:
            return manifest_data

        if "SECRETS_MANAGER_SECRETS" in os.environ:
            temporary_manifest_storage = "/var/tmp/temp_manifest_download"
            cleanup_temporary_storage(
                temporary_manifest_storage, time_delta_seconds=3600
            )
            # create a new directory to store manifest
            if not os.path.exists(temporary_manifest_storage):
                os.mkdir(temporary_manifest_storage)
            # create temporary folders for storing manifests
            download_location = create_temp_folder(
                path=temporary_manifest_storage,
                prefix=f"{self.manifest_id}-{time.time()}-",
            )
        else:
            if use_temporary_folder:
                download_location = create_temp_folder(
                    path=CONFIG.manifest_folder,
                    prefix=f"{self.manifest_id}-{time.time()}-",
                )
            else:
                download_location = CONFIG.manifest_folder

        manifest_data = self.synapse_entity_tracker.get(
            synapse_id=self.manifest_id,
            syn=self.syn,
            download_file=True,
            retrieve_if_not_present=True,
            download_location=download_location,
        )

        # This is doing a rename of the downloaded file. The reason this is important
        # is that if we are re-using a file that was previously downloaded, but the
        # file had been renamed. The file downloaded from the Synapse client is just
        # a direct copy of that renamed file. This code will set the name of the file
        # to the original name that was used to download the file. Note: An MD5 checksum
        # of the file will still be performed so if the file has changed, it will be
        # downloaded again.
        filename = manifest_data._file_handle.fileName
        if filename != os.path.basename(manifest_data.path):
            parent_folder = os.path.dirname(manifest_data.path)
            manifest_original_name_and_path = os.path.join(parent_folder, filename)

            self.syn.cache.remove(
                file_handle_id=manifest_data.dataFileHandleId, path=manifest_data.path
            )
            os.rename(manifest_data.path, manifest_original_name_and_path)
            manifest_data.path = manifest_original_name_and_path
            self.syn.cache.add(
                file_handle_id=manifest_data.dataFileHandleId,
                path=manifest_original_name_and_path,
                md5=manifest_data._file_handle.contentMd5,
            )

        return manifest_data

    def _entity_type_checking(self) -> str:
        """
        check the entity type of the id that needs to be downloaded
        Return:
             if the entity type is wrong, raise an error
        """
        # check the type of entity
        entity_type = entity_type_mapping(
            syn=self.syn,
            entity_id=self.manifest_id,
            synapse_entity_tracker=self.synapse_entity_tracker,
        )
        if entity_type != "file":
            logger.error(
                f"You are using entity type: {entity_type}. Please provide a file ID"
            )

    def download_manifest(
        self,
        newManifestName: str = "",
        manifest_df: pd.DataFrame = pd.DataFrame(),
        use_temporary_folder: bool = True,
    ) -> Union[str, File]:
        """
        Download a manifest based on a given manifest id.
        Args:
            newManifestName(optional): new name of a manifest that gets downloaded.
            manifest_df(optional): a dataframe containing name and id of manifests in a given asset view
        Return:
            manifest_data: synapse entity file object
        """

        # enables retrying if user does not have access to uncensored manifest
        # pass synID to synapseclient.Synapse.get() method to download (and overwrite) file to a location
        manifest_data = ""

        # check entity type
        self._entity_type_checking()

        # download a manifest
        try:
            manifest_data = self._download_manifest_to_folder(
                use_temporary_folder=use_temporary_folder
            )
        except (SynapseUnmetAccessRestrictions, SynapseAuthenticationError):
            # if there's an error getting an uncensored manifest, try getting the censored manifest
            if not manifest_df.empty:
                censored_regex = re.compile(".*censored.*")
                censored = manifest_df["name"].str.contains(censored_regex)
                new_manifest_id = manifest_df[censored]["id"][0]
                self.manifest_id = new_manifest_id
                try:
                    manifest_data = self._download_manifest_to_folder(
                        use_temporary_folder=use_temporary_folder
                    )
                except (
                    SynapseUnmetAccessRestrictions,
                    SynapseAuthenticationError,
                ) as e:
                    raise PermissionError(
                        "You don't have access to censored and uncensored manifests in this dataset."
                    ) from e
            else:
                logger.error(
                    f"You don't have access to the requested resource: {self.manifest_id}"
                )

        if newManifestName and os.path.exists(manifest_data.get("path")):
            # Rename the file we just made to the new name
            new_manifest_filename = newManifestName + ".csv"

            # get location of existing manifest. The manifest that will be renamed should live in the same folder as existing manifest.
            parent_folder = os.path.dirname(manifest_data.get("path"))

            new_manifest_path_name = os.path.join(parent_folder, new_manifest_filename)

            # Copy file to new location. The purpose of using a copy instead of a rename
            # is to avoid any potential issues with the file being used in another
            # process. This avoids any potential race or code cocurrency conditions.
            shutil.copyfile(src=manifest_data["path"], dst=new_manifest_path_name)

            # Adding this to cache will allow us to re-use the already downloaded
            # manifest file for up to 1 hour.
            self.syn.cache.add(
                file_handle_id=manifest_data.dataFileHandleId,
                path=new_manifest_path_name,
                md5=manifest_data._file_handle.contentMd5,
            )

            # Update file names/paths in manifest_data
            manifest_data["name"] = new_manifest_filename
            manifest_data["filename"] = new_manifest_filename
            manifest_data["path"] = new_manifest_path_name

        return manifest_data


class SynapseStorage(BaseStorage):
    """Implementation of Storage interface for datasets/files stored on Synapse.
    Provides utilities to list files in a specific project; update files annotations, create fileviews, etc.

    TODO: Need to define the interface and rename and/or refactor some of the methods below.
    """

    @tracer.start_as_current_span("SynapseStorage::__init__")
    def __init__(
        self,
        token: Optional[str] = None,  # optional parameter retrieved from browser cookie
        access_token: Optional[str] = None,
        project_scope: Optional[list] = None,
        synapse_cache_path: Optional[str] = None,
        perform_query: Optional[bool] = True,
        columns: Optional[list] = None,
        where_clauses: Optional[list] = None,
    ) -> None:
        """Initializes a SynapseStorage object.

        Args:
            token (Optional[str], optional):
              Optional token parameter as found in browser cookie upon login to synapse.
              Defaults to None.
            access_token (Optional[list], optional):
              Optional access token (personal or oauth).
              Defaults to None.
            project_scope (Optional[list], optional): Defaults to None.
            synapse_cache_path (Optional[str], optional):
              Location of synapse cache.
              Defaults to None.
        TODO:
            Consider necessity of adding "columns" and "where_clauses" params to the constructor. Currently with how `query_fileview` is implemented, these params are not needed at this step but could be useful in the future if the need for more scoped querys expands.
        """
        self.syn = self.login(synapse_cache_path, access_token)
        current_span = trace.get_current_span()
        if current_span.is_recording():
            current_span.set_attribute("user.id", self.syn.credentials.owner_id)
        self.project_scope = project_scope
        self.storageFileview = CONFIG.synapse_master_fileview_id
        self.manifest = CONFIG.synapse_manifest_basename
        self.root_synapse_cache = self.syn.cache.cache_root_dir
        self.synapse_entity_tracker = SynapseEntityTracker()
        if perform_query:
            self.query_fileview(columns=columns, where_clauses=where_clauses)

    # TODO: When moving this over to a regular cron-job the following logic should be
    # out of `manifest_download`:
    # if "SECRETS_MANAGER_SECRETS" in os.environ:
    #     temporary_manifest_storage = "/var/tmp/temp_manifest_download"
    #     cleanup_temporary_storage(temporary_manifest_storage, time_delta_seconds=3600)
    @tracer.start_as_current_span("SynapseStorage::_purge_synapse_cache")
    def _purge_synapse_cache(
        self, maximum_storage_allowed_cache_gb: int = 1, minute_buffer: int = 15
    ) -> None:
        """
        Purge synapse cache if it exceeds a certain size. Default to 1GB.
        Args:
            maximum_storage_allowed_cache_gb (int): the maximum storage allowed
              before purging cache. Default is 1 GB.
            minute_buffer (int): All files created this amount of time or older will be deleted
        """
        # try clearing the cache
        # scan a directory and check size of files
        if os.path.exists(self.root_synapse_cache):
            maximum_storage_allowed_cache_bytes = maximum_storage_allowed_cache_gb * (
                1024**3
            )
            nbytes = get_dir_size(self.root_synapse_cache)
            dir_size_bytes = check_synapse_cache_size(directory=self.root_synapse_cache)
            # if 1 GB has already been taken, purge cache before 15 min
            if dir_size_bytes >= maximum_storage_allowed_cache_bytes:
                num_of_deleted_files = clear_synapse_cache(
                    self.syn.cache, minutes=minute_buffer
                )
                logger.info(
                    f"{num_of_deleted_files}  files have been deleted from {self.root_synapse_cache}"
                )
            else:
                # on AWS, OS takes around 14-17% of our ephemeral storage (20GiB)
                # instead of guessing how much space that we left, print out .synapseCache here
                logger.info(f"the total size of .synapseCache is: {nbytes} bytes")

    @tracer.start_as_current_span("SynapseStorage::query_fileview")
    def query_fileview(
        self,
        columns: Optional[list] = None,
        where_clauses: Optional[list] = None,
        force_requery: Optional[bool] = False,
    ) -> None:
        """
        Method to query the Synapse FileView and store the results in a pandas DataFrame. The results are stored in the storageFileviewTable attribute.
        Is called once during initialization of the SynapseStorage object and can be called again later to specify a specific, more limited scope for validation purposes.
        Args:
            columns (Optional[list], optional): List of columns to be selected from the table. Defaults behavior is to request all columns.
            where_clauses (Optional[list], optional): List of where clauses to be used to scope the query. Defaults to None.
            force_requery (Optional[bool], optional): If True, forces a requery of the fileview. Defaults to False.
        """
        self._purge_synapse_cache()

        # Initialize to assume that the new fileview query will be different from what may already be stored. Initializes to True because generally one will not have already been performed
        self.new_query_different = True

        # If a query has already been performed, store the query
        previous_query_built = hasattr(self, "fileview_query")
        if previous_query_built:
            previous_query = self.fileview_query

        # Build a query with the current given parameters and check to see if it is different from the previous
        self._build_query(columns=columns, where_clauses=where_clauses)
        if previous_query_built:
            self.new_query_different = self.fileview_query != previous_query

        # Only perform the query if it is different from the previous query or we are forcing new results to be retrieved
        if self.new_query_different or force_requery:
            try:
                self.storageFileviewTable = self.syn.tableQuery(
                    query=self.fileview_query,
                ).asDataFrame(na_values=STR_NA_VALUES_FILTERED, keep_default_na=False)
            except SynapseHTTPError as exc:
                exception_text = str(exc)
                if "Unknown column path" in exception_text:
                    raise ValueError(
                        "The path column has not been added to the fileview. Please make sure that the fileview is up to date. You can add the path column to the fileview by follwing the instructions in the validation rules documentation."
                    )
                elif "Unknown column" in exception_text:
                    missing_column = exception_text.split("Unknown column ")[-1]
                    raise ValueError(
                        f"The columns {missing_column} specified in the query do not exist in the fileview. Please make sure that the column names are correct and that all expected columns have been added to the fileview."
                    )
                else:
                    raise AccessCredentialsError(self.storageFileview)

    @staticmethod
    def build_clause_from_dataset_id(
        dataset_id: Optional[str] = None, dataset_folder_list: Optional[list] = None
    ) -> str:
        """
        Method to build a where clause for a Synapse FileView query based on a dataset ID that can be used before an object is initialized.
        Args:
            dataset_id: Synapse ID of a dataset that should be used to limit the query
            dataset_folder_list: List of Synapse IDs of a dataset and all its subfolders that should be used to limit the query
        Returns:
            clause for the query or an empty string if no dataset ID is provided
        """
        # Calling this method without specifying synIDs will complete but will not scope the view
        if (not dataset_id) and (not dataset_folder_list):
            return ""

        # This will be used to gather files under a dataset recursively with a fileview query instead of walking
        if dataset_folder_list:
            search_folders = ", ".join(f"'{synId}'" for synId in dataset_folder_list)
            return f"parentId IN ({search_folders})"

        # `dataset_id` should be provided when all files are stored directly under the dataset folder
        return f"parentId='{dataset_id}'"

    def _build_query(
        self, columns: Optional[list] = None, where_clauses: Optional[list] = None
    ):
        """
        Method to build a query for Synapse FileViews
        Args:
            columns (Optional[list], optional): List of columns to be selected from the table. Defaults behavior is to request all columns.
            where_clauses (Optional[list], optional): List of where clauses to be used to scope the query. Defaults to None.
            self.storageFileview (str): Synapse FileView ID
            self.project_scope (Optional[list], optional): List of project IDs to be used to scope the query. Defaults to None.
                Gets added to where_clauses, more included for backwards compatability and as a more user friendly way of subsetting the view in a simple way.
        """
        if columns is None:
            columns = []
        if where_clauses is None:
            where_clauses = []

        if self.project_scope:
            project_scope_clause = f"projectId IN {tuple(self.project_scope + [''])}"
            where_clauses.append(project_scope_clause)

        if where_clauses:
            where_clauses = " AND ".join(where_clauses)
            where_clauses = f"WHERE {where_clauses} ;"
        else:
            where_clauses = ";"

        if columns:
            columns = ",".join(columns)
        else:
            columns = "*"

        self.fileview_query = (
            f"SELECT {columns} FROM {self.storageFileview} {where_clauses}"
        )

        return

    @staticmethod
    @tracer.start_as_current_span("SynapseStorage::login")
    def login(
        synapse_cache_path: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> synapseclient.Synapse:
        """Login to Synapse

        Args:
            access_token (Optional[str], optional): A synapse access token. Defaults to None.
            synapse_cache_path (Optional[str]): location of synapse cache

        Raises:
            ValueError: If unable to loging with access token

        Returns:
            synapseclient.Synapse: A Synapse object that is logged in
        """
        # If no token is provided, try retrieving access token from environment
        if not access_token:
            access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")

        # login using a token
        if access_token:
            try:
                syn = synapseclient.Synapse(
                    cache_root_dir=synapse_cache_path,
                    debug=False,
                    skip_checks=True,
                    cache_client=False,
                )
                syn.login(authToken=access_token, silent=True)
                current_span = trace.get_current_span()
                if current_span.is_recording():
                    current_span.set_attribute("user.id", syn.credentials.owner_id)
            except SynapseHTTPError as exc:
                raise ValueError(
                    "No access to resources. Please make sure that your token is correct"
                ) from exc
        else:
            # login using synapse credentials provided by user in .synapseConfig (default) file
            syn = synapseclient.Synapse(
                configPath=CONFIG.synapse_configuration_path,
                cache_root_dir=synapse_cache_path,
                debug=False,
                skip_checks=True,
                cache_client=False,
            )
            syn.login(silent=True)
            current_span = trace.get_current_span()
            if current_span.is_recording():
                current_span.set_attribute("user.id", syn.credentials.owner_id)
        return syn

    def missing_entity_handler(method):
        def wrapper(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except SynapseHTTPError as ex:
                str_message = str(ex).replace("\n", "")
                if "trash" in str_message or "does not exist" in str_message:
                    logging.warning(str_message)
                    return None
                else:
                    raise ex

        return wrapper

    def async_missing_entity_handler(method):
        """Decorator to handle missing entities in async methods."""

        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await method(*args, **kwargs)
            except SynapseHTTPError as ex:
                str_message = str(ex).replace("\n", "")
                if "trash" in str_message or "does not exist" in str_message:
                    logging.warning(str_message)
                    return None
                else:
                    raise ex

        return wrapper

    def getStorageFileviewTable(self):
        """Returns the storageFileviewTable obtained during initialization."""
        return self.storageFileviewTable

    def getPaginatedRestResults(self, currentUserId: str) -> Dict[str, str]:
        """Gets the paginated results of the REST call to Synapse to check what projects the current user has access to.

        Args:
            currentUserId: synapse id for the user whose projects we want to get.

        Returns:
            A dictionary with a next page token and the results.
        """
        all_results = self.syn.restGET(
            "/projects/user/{principalId}".format(principalId=currentUserId)
        )

        while (
            "nextPageToken" in all_results
        ):  # iterate over next page token in results while there is any
            results_token = self.syn.restGET(
                "/projects/user/{principalId}?nextPageToken={nextPageToken}".format(
                    principalId=currentUserId,
                    nextPageToken=all_results["nextPageToken"],
                )
            )
            all_results["results"].extend(results_token["results"])

            if "nextPageToken" in results_token:
                all_results["nextPageToken"] = results_token["nextPageToken"]
            else:
                del all_results["nextPageToken"]

        return all_results

    @tracer.start_as_current_span("SynapseStorage::getStorageProjects")
    def getStorageProjects(self, project_scope: List = None) -> list[tuple[str, str]]:
        """Gets all storage projects the current user has access to, within the scope of the 'storageFileview' attribute.

        Returns:
            A list of storage projects the current user has access to; the list consists of tuples (projectId, projectName).
        """

        # get the set of all storage Synapse project accessible for this pipeline
        storageProjects = self.storageFileviewTable["projectId"].unique()

        # get the set of storage Synapse project accessible for this user
        # get a list of projects from Synapse
        current_user_project_headers = self.synapse_entity_tracker.get_project_headers(
            current_user_id=self.syn.credentials.owner_id, syn=self.syn
        )
        project_id_to_name_dict = {}
        current_user_projects = []
        for project_header in current_user_project_headers:
            project_id_to_name_dict[project_header.get("id")] = project_header.get(
                "name"
            )
            current_user_projects.append(project_header.get("id"))

        # find set of user projects that are also in this pipeline's storage projects set
        storageProjects = list(set(storageProjects) & set(current_user_projects))

        # Limit projects to scope if specified
        if project_scope:
            storageProjects = list(set(storageProjects) & set(project_scope))

            if not storageProjects:
                raise Warning(
                    f"There are no projects that the user has access to that match the criteria of the specified project scope: {project_scope}"
                )

        # prepare a return list of project IDs and names
        projects = []
        for projectId in storageProjects:
            project_name_from_project_header = project_id_to_name_dict.get(projectId)
            projects.append((projectId, project_name_from_project_header))

        sorted_projects_list = sorted(projects, key=lambda tup: tup[0])

        return sorted_projects_list

    @tracer.start_as_current_span("SynapseStorage::getStorageDatasetsInProject")
    def getStorageDatasetsInProject(self, projectId: str) -> list[tuple[str, str]]:
        """Gets all datasets in folder under a given storage project that the current user has access to.

        Args:
            projectId: synapse ID of a storage project.

        Returns:
            A list of datasets within the given storage project; the list consists of tuples (datasetId, datasetName).
            None: If the projectId cannot be found on Synapse.
        """

        # select all folders and fetch their names from within the storage project;
        # if folder content type is defined, only select folders that contain datasets
        if "contentType" in self.storageFileviewTable.columns:
            foldersTable = self.storageFileviewTable[
                (self.storageFileviewTable["contentType"] == "dataset")
                & (self.storageFileviewTable["projectId"] == projectId)
            ]
        else:
            foldersTable = self.storageFileviewTable[
                (self.storageFileviewTable["type"] == "folder")
                & (self.storageFileviewTable["parentId"] == projectId)
            ]

        # get an array of tuples (folderId, folderName)
        # some folders are part of datasets; others contain datasets
        # each dataset parent is the project; folders part of a dataset have another folder as a parent
        # to get folders if and only if they contain datasets for each folder
        # check if folder's parent is the project; if so that folder contains a dataset,
        # unless the folder list has already been filtered to dataset folders based on contentType attribute above

        datasetList = []
        folderProperties = ["id", "name"]
        for folder in list(
            foldersTable[folderProperties].itertuples(index=False, name=None)
        ):
            datasetList.append(folder)

        sorted_dataset_list = sorted(datasetList, key=lambda tup: tup[0])

        return sorted_dataset_list

    @tracer.start_as_current_span("SynapseStorage::getFilesInStorageDataset")
    def getFilesInStorageDataset(
        self, datasetId: str, fileNames: List = None, fullpath: bool = True
    ) -> List[Tuple[str, str]]:
        """Gets all files (excluding manifest files) in a given dataset folder.

        Args:
            datasetId: synapse ID of a storage dataset.
            fileNames: get a list of files with particular names; defaults to None in which case all dataset files are returned (except bookkeeping files, e.g.
            metadata manifests); if fileNames is not None, all files matching the names in the fileNames list are returned if present.
            fullpath: if True return the full path as part of this filename; otherwise return just base filename

        Returns:
            A list of files; the list consists of tuples (fileId, fileName).

        Raises:
            ValueError: Dataset ID not found.
        """
        file_list = []

        # Get path to dataset folder by using childern to avoid cases where the dataset is the scope of the view
        if self.storageFileviewTable.empty:
            raise ValueError(
                f"Fileview {self.storageFileview} is empty, please check the table and the provided synID and try again."
            )

        child_path = self.storageFileviewTable.loc[
            self.storageFileviewTable["parentId"] == datasetId, "path"
        ]
        if child_path.empty:
            raise LookupError(
                f"Dataset {datasetId} could not be found in fileview {self.storageFileview}."
            )
        child_path = child_path.iloc[0]

        # Get the dataset path by eliminating the child's portion of the path to account for nested datasets
        parent = child_path.split("/")[:-1]
        parent = "/".join(parent)

        # Format dataset path to be used in table query
        dataset_path = f"'{parent}/%'"

        # When querying, only include files to exclude entity files and subdirectories
        where_clauses = [f"path like {dataset_path}", "type='file'"]

        # Requery the fileview to specifically get the files in the given dataset
        self.query_fileview(columns=["id", "path"], where_clauses=where_clauses)

        # Exclude manifest files
        non_manifest_files = self.storageFileviewTable.loc[
            ~self.storageFileviewTable["path"].str.contains("synapse_storage_manifest"),
            :,
        ]

        # Remove all files that are not in the list of fileNames
        if fileNames:
            filename_regex = "|".join(fileNames)

            matching_files = non_manifest_files["path"].str.contains(
                filename_regex, case=False, regex=True
            )

            non_manifest_files = non_manifest_files.loc[matching_files, :]

        # Truncate path if necessary
        if not fullpath:
            non_manifest_files.path = non_manifest_files.path.apply(os.path.basename)

        # Return list of files as expected by other methods
        file_list = list(non_manifest_files.itertuples(index=False, name=None))

        return file_list

    def _get_manifest_id(self, manifest: pd.DataFrame) -> str:
        """If both censored and uncensored manifests are present, return uncensored manifest; if only one manifest is present, return manifest id of that manifest; if more than two manifests are present, return the manifest id of the first one.
        Args:
        manifest: a dataframe contains name and id of manifests in a given asset view

        Return:
        manifest_syn_id: id of a given censored or uncensored manifest
        """
        censored_regex = re.compile(".*censored.*")
        censored = manifest["name"].str.contains(censored_regex)
        if any(censored):
            # Try to use uncensored manifest first
            not_censored = ~censored
            if any(not_censored):
                manifest_syn_id = manifest[not_censored]["id"].iloc[0]
            # if only censored manifests are available, just use the first censored manifest
            else:
                manifest_syn_id = manifest["id"].iloc[0]

        # otherwise, use the first (implied only) version that exists
        else:
            manifest_syn_id = manifest["id"].iloc[0]

        return manifest_syn_id

    @tracer.start_as_current_span("SynapseStorage::getDatasetManifest")
    def getDatasetManifest(
        self,
        datasetId: str,
        downloadFile: bool = False,
        newManifestName: str = "",
        use_temporary_folder: bool = True,
    ) -> Union[str, File]:
        """Gets the manifest associated with a given dataset.

        Args:
            datasetId: synapse ID of a storage dataset.
            downloadFile: boolean argument indicating if manifest file in dataset should be downloaded or not.
            newManifestName: new name of a manifest that gets downloaded
            use_temporary_folder: boolean argument indicating if a temporary folder
                should be used to store the manifest file. This is useful when running
                this code as an API server where multiple requests could be made at the
                same time. This is set to False when the code is being used from the
                CLI. Defaults to True.

        Returns:
            manifest_syn_id (String): Synapse ID of exisiting manifest file.
            manifest_data (synapseclient.entity.File): Synapse entity if downloadFile is True.
            "" (String): No pre-exisiting manifest in dataset.
        """
        manifest_data = ""

        # get a list of files containing the manifest for this dataset (if any)
        all_files = self.storageFileviewTable

        # construct regex based on manifest basename in the config
        manifest_re = re.compile(os.path.basename(self.manifest) + ".*.[tc]sv")

        # search manifest based on given manifest basename regex above
        # and return a dataframe containing name and id of manifests in a given asset view
        manifest = all_files[
            (all_files["name"].str.contains(manifest_re, regex=True))
            & (all_files["parentId"] == datasetId)
        ]

        manifest = manifest[["id", "name"]]

        # if there is no pre-exisiting manifest in the specified dataset
        if manifest.empty:
            logger.warning(
                f"Could not find a manifest that fits basename {self.manifest} in asset view and dataset {datasetId}"
            )
            return ""

        # if there is an exisiting manifest
        else:
            manifest_syn_id = self._get_manifest_id(manifest)
            if downloadFile:
                md = ManifestDownload(
                    self.syn,
                    manifest_id=manifest_syn_id,
                    synapse_entity_tracker=self.synapse_entity_tracker,
                )
                manifest_data = md.download_manifest(
                    newManifestName=newManifestName,
                    manifest_df=manifest,
                    use_temporary_folder=use_temporary_folder,
                )
                # TO DO: revisit how downstream code handle manifest_data. If the downstream code would break when manifest_data is an empty string,
                # then we should catch the error here without returning an empty string.
                if not manifest_data:
                    logger.debug(
                        f"No manifest data returned. Please check if you have successfully downloaded manifest: {manifest_syn_id}"
                    )
                return manifest_data
            return manifest_syn_id

    def getDataTypeFromManifest(self, manifestId: str):
        """Fetch a manifest and return data types of all columns
        Args:
            manifestId: synapse ID of a manifest
        """
        # get manifest file path
        manifest_entity = self.synapse_entity_tracker.get(
            synapse_id=manifestId, syn=self.syn, download_file=True
        )
        manifest_filepath = manifest_entity.path

        # load manifest dataframe
        manifest = load_df(
            manifest_filepath,
            preserve_raw_input=False,
            data_model=False,
        )

        # convert the dataFrame to use best possible dtypes.
        manifest_new = manifest.convert_dtypes()

        # get data types of columns
        result = manifest_new.dtypes.to_frame("dtypes").reset_index()

        # return the result as a dictionary
        result_dict = result.set_index("index")["dtypes"].astype(str).to_dict()

        return result_dict

    def _get_files_metadata_from_dataset(
        self, datasetId: str, only_new_files: bool, manifest: pd.DataFrame = None
    ) -> Optional[dict]:
        """retrieve file ids under a particular datasetId

        Args:
            datasetId (str): a dataset id
            only_new_files (bool): if only adding new files that are not already exist
            manifest (pd.DataFrame): metadata manifest dataframe. Default to None.

        Returns:
            a dictionary that contains filename and entityid under a given datasetId or None if there is nothing under a given dataset id are not available
        """
        dataset_files = self.getFilesInStorageDataset(datasetId)
        if dataset_files:
            dataset_file_names_id_dict = self._get_file_entityIds(
                dataset_files, only_new_files=only_new_files, manifest=manifest
            )
            return dataset_file_names_id_dict
        else:
            return None

    def add_entity_id_and_filename(
        self, datasetId: str, manifest: pd.DataFrame
    ) -> pd.DataFrame:
        """add entityid and filename column to an existing manifest assuming entityId column is not already present

        Args:
            datasetId (str): dataset syn id
            manifest (pd.DataFrame): existing manifest dataframe, assuming this dataframe does not have an entityId column and Filename column is present but completely empty

        Returns:
            pd.DataFrame: returns a pandas dataframe
        """
        # get file names and entity ids of a given dataset
        dataset_files_dict = self._get_files_metadata_from_dataset(
            datasetId, only_new_files=False
        )

        if dataset_files_dict:
            # turn manifest dataframe back to a dictionary for operation
            manifest_dict = manifest.to_dict("list")

            # update Filename column
            # add entityId column to the end
            manifest_dict.update(dataset_files_dict)

            # if the component column exists in existing manifest, fill up that column
            if "Component" in manifest_dict.keys():
                manifest_dict["Component"] = manifest_dict["Component"] * max(
                    1, len(manifest_dict["Filename"])
                )

            # turn dictionary back to a dataframe
            manifest_df_index = pd.DataFrame.from_dict(manifest_dict, orient="index")
            manifest_df_updated = manifest_df_index.transpose()

            # fill na with empty string
            manifest_df_updated = manifest_df_updated.fillna("")

            # drop index
            manifest_df_updated = manifest_df_updated.reset_index(drop=True)

            return manifest_df_updated
        else:
            return manifest

    def fill_in_entity_id_filename(
        self, datasetId: str, manifest: pd.DataFrame
    ) -> Tuple[List, pd.DataFrame]:
        """fill in Filename column and EntityId column. EntityId column and Filename column will be created if not already present.

        Args:
            datasetId (str): dataset syn id
            manifest (pd.DataFrame): existing manifest dataframe.

        Returns:
            Tuple[List, pd.DataFrame]: a list of synIds that are under a given datasetId folder and updated manifest dataframe
        """
        # get dataset file names and entity id as a list of tuple
        dataset_files = self.getFilesInStorageDataset(datasetId)

        # update manifest with additional filenames, if any
        # note that if there is an existing manifest and there are files in the dataset
        # the columns Filename and entityId are assumed to be present in manifest schema
        # TODO: use idiomatic panda syntax
        if not dataset_files:
            manifest = manifest.fillna("")
            return dataset_files, manifest

        all_files = self._get_file_entityIds(
            dataset_files=dataset_files, only_new_files=False, manifest=manifest
        )
        new_files = self._get_file_entityIds(
            dataset_files=dataset_files, only_new_files=True, manifest=manifest
        )

        all_files = pd.DataFrame(all_files)
        new_files = pd.DataFrame(new_files)

        # update manifest so that it contains new dataset files
        manifest = (
            pd.concat([manifest, new_files], sort=False)
            .reset_index()
            .drop("index", axis=1)
        )

        # Reindex manifest and new files dataframes according to entityIds to align file paths and metadata
        manifest_reindex = manifest.set_index("entityId")
        all_files_reindex = all_files.set_index("entityId")
        all_files_reindex_like_manifest = all_files_reindex.reindex_like(
            manifest_reindex
        )

        # Check if individual file paths in manifest and from synapse match
        file_paths_match = (
            manifest_reindex["Filename"] == all_files_reindex_like_manifest["Filename"]
        )

        # If all the paths do not match, update the manifest with the filepaths from synapse
        if not file_paths_match.all():
            manifest_reindex.loc[
                ~file_paths_match, "Filename"
            ] = all_files_reindex_like_manifest.loc[~file_paths_match, "Filename"]

            # reformat manifest for further use
            manifest = manifest_reindex.reset_index()
            entityIdCol = manifest.pop("entityId")
            manifest.insert(len(manifest.columns), "entityId", entityIdCol)

        manifest = manifest.fillna("")
        return dataset_files, manifest

    @tracer.start_as_current_span("SynapseStorage::updateDatasetManifestFiles")
    def updateDatasetManifestFiles(
        self, dmge: DataModelGraphExplorer, datasetId: str, store: bool = True
    ) -> Union[Tuple[str, pd.DataFrame], None]:
        """Fetch the names and entity IDs of all current files in dataset in store, if any; update dataset's manifest with new files, if any.

        Args:
            dmge: DataModelGraphExplorer Instance
            datasetId: synapse ID of a storage dataset.
            store: if set to True store updated manifest in asset store; if set to False
            return a Pandas dataframe containing updated manifest but do not store to asset store


        Returns:
            Synapse ID of updated manifest and Pandas dataframe containing the updated manifest.
            If there is no existing manifest or if the manifest does not have an entityId column, return None
        """

        # get existing manifest Synapse ID
        manifest_id = self.getDatasetManifest(datasetId)

        # if there is no manifest return None
        if not manifest_id:
            return None

        manifest_entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_id, syn=self.syn, download_file=True
        )
        manifest_filepath = manifest_entity.path
        manifest = load_df(manifest_filepath)

        # If the manifest does not have an entityId column, trigger a new manifest to be generated
        if "entityId" not in manifest.columns:
            return None

        manifest_is_file_based = "Filename" in manifest.columns

        if manifest_is_file_based:
            # update manifest with additional filenames, if any
            # note that if there is an existing manifest and there are files in the dataset
            # the columns Filename and entityId are assumed to be present in manifest schema
            # TODO: use idiomatic panda syntax
            dataset_files, manifest = self.fill_in_entity_id_filename(
                datasetId, manifest
            )
            if dataset_files:
                # update the manifest file, so that it contains the relevant entity IDs
                if store:
                    manifest.to_csv(manifest_filepath, index=False)

                    # store manifest and update associated metadata with manifest on Synapse
                    manifest_id = self.associateMetadataWithFiles(
                        dmge, manifest_filepath, datasetId
                    )

        return manifest_id, manifest

    def _get_file_entityIds(
        self,
        dataset_files: List,
        only_new_files: bool = False,
        manifest: pd.DataFrame = None,
    ):
        """
        Get a dictionary of files in a dataset. Either files that are not in the current manifest or all files

        Args:
            manifest: metadata manifest
            dataset_file: List of all files in a dataset
            only_new_files: boolean to control whether only new files are returned or all files in the dataset
        Returns:
            files: dictionary of file names and entityIDs, with scope as specified by `only_new_files`
        """
        files = {"Filename": [], "entityId": []}

        if only_new_files:
            if manifest is None:
                raise UnboundLocalError(
                    "No manifest was passed in, a manifest is required when `only_new_files` is True."
                )

            if "entityId" not in manifest.columns:
                raise ValueError(
                    "The manifest in your dataset and/or top level folder must contain the 'entityId' column. "
                    "Please generate an empty manifest without annotations, manually add annotations to the "
                    "appropriate files in the manifest, and then try again."
                )

            # find new files (that are not in the current manifest) if any
            for file_id, file_name in dataset_files:
                if not file_id in manifest["entityId"].values:
                    files["Filename"].append(file_name)
                    files["entityId"].append(file_id)
        else:
            # get all files
            for file_id, file_name in dataset_files:
                files["Filename"].append(file_name)
                files["entityId"].append(file_id)

        return files

    @tracer.start_as_current_span("SynapseStorage::getProjectManifests")
    def getProjectManifests(
        self, projectId: str
    ) -> list[tuple[tuple[str, str], tuple[str, str], tuple[str, str]]]:
        """Gets all metadata manifest files across all datasets in a specified project.

        Returns: A list of datasets per project; metadata manifest Synapse ID for each dataset; and the corresponding schema component of the manifest
                 as a list of tuples, one for each manifest:
                    [
                        (
                            (datasetId, dataName),
                            (manifestId, manifestName),
                            (componentSchemaLabel, componentSchemaLabel) TODO: # get component name from schema
                        ),
                        ...
                    ]

        TODO: Return manifest URI instead of Synapse ID for interoperability with other implementations of a store interface
        """
        component = None
        entity = None
        manifests = []

        datasets = self.getStorageDatasetsInProject(projectId)

        for datasetId, datasetName in datasets:
            # encode information about the manifest in a simple list (so that R clients can unpack it)
            # eventually can serialize differently

            # Get synID of manifest for a dataset
            manifestId = self.getDatasetManifest(datasetId)

            # If a manifest exists, get the annotations for it, else return base 'manifest' tuple
            if manifestId:
                annotations = self.getFileAnnotations(manifestId)

                # If manifest has annotations specifying component, use that
                if annotations and "Component" in annotations:
                    component = annotations["Component"]
                    entity = self.synapse_entity_tracker.get(
                        synapse_id=manifestId, syn=self.syn, download_file=False
                    )
                    manifest_name = entity["properties"]["name"]

                # otherwise download the manifest and parse for information
                elif not annotations or "Component" not in annotations:
                    logging.debug(
                        f"No component annotations have been found for manifest {manifestId}. "
                        "The manifest will be downloaded and parsed instead. "
                        "For increased speed, add component annotations to manifest."
                    )

                    manifest_info = self.getDatasetManifest(
                        datasetId, downloadFile=True
                    )
                    manifest_name = manifest_info["properties"].get("name", "")

                    if not manifest_name:
                        logger.error(f"Failed to download manifests from {datasetId}")

                    manifest_path = manifest_info["path"]

                    manifest_df = load_df(manifest_path)

                    # Get component from component column if it exists
                    if (
                        "Component" in manifest_df
                        and not manifest_df["Component"].empty
                    ):
                        list(set(manifest_df["Component"]))
                        component = list(set(manifest_df["Component"]))

                        # Added to address issues raised during DCA testing
                        if "" in component:
                            component.remove("")

                        if len(component) == 1:
                            component = component[0]
                        elif len(component) > 1:
                            logging.warning(
                                f"Manifest {manifestId} is composed of multiple components. Schematic does not support mulit-component manifests at this time."
                                "Behavior of manifests with multiple components is undefined"
                            )
            else:
                manifest_name = ""
                component = None
            if component:
                manifest = (
                    (datasetId, datasetName),
                    (manifestId, manifest_name),
                    (component, component),
                )
            elif manifestId:
                logging.debug(
                    f"Manifest {manifestId} does not have an associated Component"
                )
                manifest = (
                    (datasetId, datasetName),
                    (manifestId, manifest_name),
                    ("", ""),
                )
            else:
                manifest = (
                    (datasetId, datasetName),
                    ("", ""),
                    ("", ""),
                )

            if manifest:
                manifests.append(manifest)

        return manifests

    def upload_project_manifests_to_synapse(
        self, dmge: DataModelGraphExplorer, projectId: str
    ) -> List[str]:
        """Upload all metadata manifest files across all datasets in a specified project as tables in Synapse.

        Returns: String of all the manifest_table_ids of all the manifests that have been loaded.
        """

        manifests = []
        manifest_loaded = []
        datasets = self.getStorageDatasetsInProject(projectId)

        for datasetId, datasetName in datasets:
            # encode information about the manifest in a simple list (so that R clients can unpack it)
            # eventually can serialize differently

            manifest = ((datasetId, datasetName), ("", ""), ("", ""))

            manifest_info = self.getDatasetManifest(datasetId, downloadFile=True)
            if manifest_info:
                manifest_id = manifest_info["properties"]["id"]
                manifest_name = manifest_info["properties"]["name"]
                manifest_path = manifest_info["path"]
                manifest_df = load_df(manifest_path)
                manifest_table_id = uploadDB(
                    dmge=dmge,
                    manifest=manifest,
                    datasetId=datasetId,
                    table_name=datasetName,
                )
                manifest_loaded.append(datasetName)
        return manifest_loaded

    def upload_annotated_project_manifests_to_synapse(
        self, projectId: str, path_to_json_ld: str, dry_run: bool = False
    ) -> List[str]:
        """
        Purpose:
            For all manifests in a project, upload them as a table and add annotations manifest csv.
            Assumes the manifest is already present as a CSV in a dataset in the project.

        """
        # Instantiate DataModelParser
        data_model_parser = DataModelParser(path_to_data_model=path_to_json_ld)
        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(parsed_data_model)

        # Generate graph
        graph_data_model = data_model_grapher.generate_data_model_graph()

        # Instantiate DataModelGraphExplorer
        dmge = DataModelGraphExplorer(graph_data_model)

        manifests = []
        manifest_loaded = []
        datasets = self.getStorageDatasetsInProject(projectId)
        for datasetId, datasetName in datasets:
            # encode information about the manifest in a simple list (so that R clients can unpack it)
            # eventually can serialize differently

            manifest = ((datasetId, datasetName), ("", ""), ("", ""))
            manifests.append(manifest)

            manifest_info = self.getDatasetManifest(datasetId, downloadFile=True)

            if manifest_info:
                manifest_id = manifest_info["properties"]["id"]
                manifest_name = manifest_info["properties"]["name"]
                manifest_path = manifest_info["path"]
                manifest = (
                    (datasetId, datasetName),
                    (manifest_id, manifest_name),
                    ("", ""),
                )
                if not dry_run:
                    self.associateMetadataWithFiles(
                        dmge, manifest_path, datasetId, manifest_record_type="table"
                    )
                manifest_loaded.append(manifest)

        return manifests, manifest_loaded

    def move_entities_to_new_project(
        self,
        projectId: str,
        newProjectId: str,
        returnEntities: bool = False,
        dry_run: bool = False,
    ):
        """
        For each manifest csv in a project, look for all the entitiy ids that are associated.
        Look up the entitiy in the files, move the entity to new project.
        """

        manifests = []
        manifest_loaded = []
        datasets = self.getStorageDatasetsInProject(projectId)
        if datasets:
            for datasetId, datasetName in datasets:
                # encode information about the manifest in a simple list (so that R clients can unpack it)
                # eventually can serialize differently

                manifest = ((datasetId, datasetName), ("", ""), ("", ""))
                manifests.append(manifest)

                manifest_info = self.getDatasetManifest(datasetId, downloadFile=True)
                if manifest_info:
                    manifest_id = manifest_info["properties"]["id"]
                    manifest_name = manifest_info["properties"]["name"]
                    manifest_path = manifest_info["path"]
                    manifest_df = load_df(manifest_path)

                    manifest = (
                        (datasetId, datasetName),
                        (manifest_id, manifest_name),
                        ("", ""),
                    )
                    manifest_loaded.append(manifest)

                    annotation_entities = self.storageFileviewTable[
                        (self.storageFileviewTable["id"].isin(manifest_df["entityId"]))
                        & (self.storageFileviewTable["type"] == "folder")
                    ]["id"]

                    if returnEntities:
                        for entityId in annotation_entities:
                            if not dry_run:
                                moved_entity = self.syn.move(entityId, datasetId)
                                self.synapse_entity_tracker.add(
                                    synapse_id=moved_entity.id, entity=moved_entity
                                )
                            else:
                                logging.info(
                                    f"{entityId} will be moved to folder {datasetId}."
                                )
                    else:
                        # generate project folder
                        archive_project_folder = Folder(
                            projectId + "_archive", parent=newProjectId
                        )
                        archive_project_folder = self.syn.store(archive_project_folder)
                        self.synapse_entity_tracker.add(
                            synapse_id=archive_project_folder.id,
                            entity=archive_project_folder,
                        )

                        # generate dataset folder
                        dataset_archive_folder = Folder(
                            "_".join([datasetId, datasetName, "archive"]),
                            parent=archive_project_folder.id,
                        )
                        dataset_archive_folder = self.syn.store(dataset_archive_folder)
                        self.synapse_entity_tracker.add(
                            synapse_id=dataset_archive_folder.id,
                            entity=dataset_archive_folder,
                        )

                        for entityId in annotation_entities:
                            # move entities to folder
                            if not dry_run:
                                moved_entity = self.syn.move(
                                    entityId, dataset_archive_folder.id
                                )
                                self.synapse_entity_tracker.add(
                                    synapse_id=moved_entity.id, entity=moved_entity
                                )
                            else:
                                logging.info(
                                    f"{entityId} will be moved to folder {dataset_archive_folder.id}."
                                )
        else:
            raise LookupError(
                f"No datasets were found in the specified project: {projectId}. Re-check specified master_fileview in CONFIG and retry."
            )
        return manifests, manifest_loaded

    @tracer.start_as_current_span("SynapseStorage::get_synapse_table")
    def get_synapse_table(self, synapse_id: str) -> Tuple[pd.DataFrame, CsvFileTable]:
        """Download synapse table as a pd dataframe; return table schema and etags as results too

        Args:
            synapse_id: synapse ID of the table to query
        """

        results = self.syn.tableQuery("SELECT * FROM {}".format(synapse_id))
        df = results.asDataFrame(
            rowIdAndVersionInIndex=False,
            na_values=STR_NA_VALUES_FILTERED,
            keep_default_na=False,
        )

        return df, results

    @missing_entity_handler
    @tracer.start_as_current_span("SynapseStorage::uploadDB")
    def uploadDB(
        self,
        dmge: DataModelGraphExplorer,
        manifest: pd.DataFrame,
        datasetId: str,
        table_name: str,
        restrict: bool = False,
        table_manipulation: str = "replace",
        table_column_names: str = "class_label",
    ):
        """
        Method to upload a database to an asset store. In synapse, this will upload a metadata table

        Args:
            dmge: DataModelGraphExplorer object
            manifest: pd.Df manifest to upload
            datasetId: synID of the dataset for the manifest
            table_name: name of the table to be uploaded
            restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions
            existingTableId: str of the synId of the existing table, if one already exists
            table_manipulation: str, 'replace' or 'upsert', in the case where a manifest already exists, should the new metadata replace the existing (replace) or be added to it (upsert)
            table_column_names: (str): display_name/display_label/class_label (default). Sets labeling style for table column names. display_name will use the raw display name as the column name. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting.
        Returns:
            manifest_table_id: synID of the uploaded table
            manifest: the original manifset
            table_manifest: manifest formatted appropriately for the table

        """

        col_schema, table_manifest = self.formatDB(
            dmge=dmge, manifest=manifest, table_column_names=table_column_names
        )

        manifest_table_id = self.buildDB(
            datasetId,
            table_name,
            col_schema,
            table_manifest,
            table_manipulation,
            dmge,
            restrict,
        )

        return manifest_table_id, manifest, table_manifest

    @tracer.start_as_current_span("SynapseStorage::formatDB")
    def formatDB(self, dmge, manifest, table_column_names):
        """
        Method to format a manifest appropriatly for upload as table

        Args:
            dmge: DataModelGraphExplorer object
            manifest: pd.Df manifest to upload
            table_column_names: (str): display_name/display_label/class_label (default). Sets labeling style for table column names. display_name will use the raw display name as the column name. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting.
        Returns:
            col_schema: schema for table columns: type, size, etc
            table_manifest: formatted manifest

        """
        # Rename the manifest columns to display names to match fileview

        blacklist_chars = ["(", ")", ".", " ", "-"]
        manifest_columns = manifest.columns.tolist()

        table_manifest = deepcopy(manifest)

        if table_column_names == "display_name":
            cols = table_manifest.columns

        elif table_column_names == "display_label":
            cols = [
                str(col).translate({ord(x): "" for x in blacklist_chars})
                for col in manifest_columns
            ]

        elif table_column_names == "class_label":
            cols = [
                get_class_label_from_display_name(str(col)).translate(
                    {ord(x): "" for x in blacklist_chars}
                )
                for col in manifest_columns
            ]
        else:
            ValueError(
                f"The provided table_column_name: {table_column_names} is not valid, please resubmit with an allowed value only."
            )

        cols = list(map(lambda x: x.replace("EntityId", "entityId"), cols))

        # Reset column names in table manifest
        table_manifest.columns = cols

        # move entity id to end of df
        entity_col = table_manifest.pop("entityId")
        table_manifest.insert(len(table_manifest.columns), "entityId", entity_col)

        # Get the column schema
        col_schema = as_table_columns(table_manifest)

        # Set Id column length to 64 (for some reason not being auto set.)
        for i, col in enumerate(col_schema):
            if col["name"].lower() == "id":
                col_schema[i]["maximumSize"] = 64

        return col_schema, table_manifest

    @tracer.start_as_current_span("SynapseStorage::buildDB")
    def buildDB(
        self,
        datasetId: str,
        table_name: str,
        col_schema: List,
        table_manifest: pd.DataFrame,
        table_manipulation: str,
        dmge: DataModelGraphExplorer,
        restrict: bool = False,
    ):
        """
        Method to construct the table appropriately: create new table, replace existing, or upsert new into existing
        Calls TableOperations class to execute

        Args:
            datasetId: synID of the dataset for the manifest
            table_name: name of the table to be uploaded
            col_schema: schema for table columns: type, size, etc from `formatDB`
            table_manifest: formatted manifest that can be uploaded as a table
            table_manipulation: str, 'replace' or 'upsert', in the case where a manifest already exists, should the new metadata replace the existing (replace) or be added to it (upsert)
            restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions

        Returns:
            manifest_table_id: synID of the uploaded table

        """
        table_parent_id = self.getDatasetProject(datasetId=datasetId)
        existing_table_id = self.syn.findEntityId(
            name=table_name, parent=table_parent_id
        )

        tableOps = TableOperations(
            synStore=self,
            tableToLoad=table_manifest,
            tableName=table_name,
            datasetId=datasetId,
            existingTableId=existing_table_id,
            restrict=restrict,
            synapse_entity_tracker=self.synapse_entity_tracker,
        )

        if not table_manipulation or existing_table_id is None:
            manifest_table_id = tableOps.createTable(
                columnTypeDict=col_schema,
                specifySchema=True,
            )
        elif existing_table_id is not None:
            if table_manipulation.lower() == "replace":
                manifest_table_id = tableOps.replaceTable(
                    specifySchema=True,
                    columnTypeDict=col_schema,
                )
            elif table_manipulation.lower() == "upsert":
                manifest_table_id = tableOps.upsertTable(
                    dmge=dmge,
                )
            elif table_manipulation.lower() == "update":
                manifest_table_id = tableOps.updateTable()

        if table_manipulation and table_manipulation.lower() == "upsert":
            table_entity = self.synapse_entity_tracker.get(
                synapse_id=existing_table_id or manifest_table_id,
                syn=self.syn,
                download_file=False,
            )
            annos = OldAnnotations(
                id=table_entity.id,
                etag=table_entity.etag,
                values=table_entity.annotations,
            )
            annos["primary_key"] = table_manifest["Component"][0] + "_id"
            annos = self.syn.set_annotations(annos)
            table_entity.etag = annos.etag
            table_entity.annotations = annos

        return manifest_table_id

    @tracer.start_as_current_span("SynapseStorage::upload_manifest_file")
    def upload_manifest_file(
        self,
        manifest,
        metadataManifestPath,
        datasetId,
        restrict_manifest,
        component_name="",
    ):
        # Update manifest to have the new entityId column
        manifest.to_csv(metadataManifestPath, index=False)

        # store manifest to Synapse as a CSV
        # update file name
        file_name_full = metadataManifestPath.split("/")[-1]
        file_extension = file_name_full.split(".")[-1]

        # Differentiate "censored" and "uncensored" manifest
        if "censored" in file_name_full:
            file_name_new = (
                os.path.basename(CONFIG.synapse_manifest_basename)
                + "_"
                + component_name
                + "_censored"
                + "."
                + file_extension
            )
        else:
            file_name_new = (
                os.path.basename(CONFIG.synapse_manifest_basename)
                + "_"
                + component_name
                + "."
                + file_extension
            )

        manifest_synapse_file = None
        try:
            # Rename the file to file_name_new then revert
            # This is to maintain the original file name in-case other code is
            # expecting that the file exists with the original name
            original_file_path = metadataManifestPath
            new_file_path = os.path.join(
                os.path.dirname(metadataManifestPath), file_name_new
            )
            os.rename(original_file_path, new_file_path)

            manifest_synapse_file = self._store_file_for_manifest_upload(
                new_file_path=new_file_path,
                dataset_id=datasetId,
                existing_file_name=file_name_full,
                file_name_new=file_name_new,
                restrict_manifest=restrict_manifest,
            )
            manifest_synapse_file_id = manifest_synapse_file.id

        finally:
            # Revert the file name back to the original
            os.rename(new_file_path, original_file_path)

            if manifest_synapse_file:
                manifest_synapse_file.path = original_file_path

        return manifest_synapse_file_id

    def _store_file_for_manifest_upload(
        self,
        new_file_path: str,
        dataset_id: str,
        existing_file_name: str,
        file_name_new: str,
        restrict_manifest: bool,
    ) -> File:
        """Handles a create or update of a manifest file that is going to be uploaded.
        If we already have a copy of the Entity in memory we will update that instance,
        otherwise create a new File instance to be created in Synapse. Once stored
        this will add the file to the `synapse_entity_tracker` for future reference.

        Args:
            new_file_path (str): The path to the new manifest file
            dataset_id (str): The Synapse ID of the dataset the manifest is associated with
            existing_file_name (str): The name of the existing file
            file_name_new (str): The name of the new file
            restrict_manifest (bool): Whether the manifest should be restricted

        Returns:
            File: The stored manifest file
        """
        local_tracked_file_instance = (
            self.synapse_entity_tracker.search_local_by_parent_and_name(
                name=existing_file_name, parent_id=dataset_id
            )
            or self.synapse_entity_tracker.search_local_by_parent_and_name(
                name=file_name_new, parent_id=dataset_id
            )
        )

        if local_tracked_file_instance:
            local_tracked_file_instance.path = new_file_path
            local_tracked_file_instance.description = (
                "Manifest for dataset " + dataset_id
            )
            manifest_synapse_file = local_tracked_file_instance
        else:
            manifest_synapse_file = File(
                path=new_file_path,
                description="Manifest for dataset " + dataset_id,
                parent=dataset_id,
                name=file_name_new,
            )

        manifest_synapse_file = self.syn.store(
            manifest_synapse_file, isRestricted=restrict_manifest
        )

        self.synapse_entity_tracker.add(
            synapse_id=manifest_synapse_file.id, entity=manifest_synapse_file
        )
        return manifest_synapse_file

    async def get_async_annotation(self, synapse_id: str) -> Dict[str, Any]:
        """get annotations asynchronously

        Args:
            synapse_id (str): synapse id of the entity that the annotation belongs

        Returns:
            Dict[str, Any]: The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
        """
        return await get_entity_id_bundle2(
            entity_id=synapse_id,
            request={"includeAnnotations": True},
            synapse_client=self.syn,
        )

    async def store_async_annotation(self, annotation_dict: dict) -> Annotations:
        """store annotation in an async way

        Args:
            annotation_dict (dict): annotation in a dictionary format

        Returns:
            Annotations: The stored annotations.
        """
        annotation_data = Annotations.from_dict(
            synapse_annotations=annotation_dict["annotations"]["annotations"]
        )
        annotation_class = Annotations(
            annotations=annotation_data,
            etag=annotation_dict["annotations"]["etag"],
            id=annotation_dict["annotations"]["id"],
        )
        annotation_storage_result = await annotation_class.store_async(
            synapse_client=self.syn
        )
        local_entity = self.synapse_entity_tracker.get(
            synapse_id=annotation_dict["annotations"]["id"],
            syn=self.syn,
            download_file=False,
            retrieve_if_not_present=False,
        )
        if local_entity:
            local_entity.etag = annotation_storage_result.etag
            local_entity.annotations = annotation_storage_result
        return annotation_storage_result

    def process_row_annotations(
        self,
        dmge: DataModelGraphExplorer,
        metadata_syn: Dict[str, Any],
        hide_blanks: bool,
        csv_list_regex: str,
        annos: Dict[str, Any],
        annotation_keys: str,
    ) -> Dict[str, Any]:
        """Processes metadata annotations based on the logic below:
        1. Checks if the hide_blanks flag is True, and if the current annotation value (anno_v) is:
            An empty or whitespace-only string.
            A NaN value (if the annotation is a float).
        if any of the above conditions are met, and hide_blanks is True, the annotation key is not going to be uploaded and skips further processing of that annotation key.
        if any of the above conditions are met, and hide_blanks is False, assigns an empty string "" as the annotation value for that key.

        2. If the value is a string and matches the pattern defined by csv_list_regex, get validation rule based on "node label" or "node display name".
        Check if the rule contains "list" as a rule, if it does, split the string by comma and assign the resulting list as the annotation value for that key.

        3. For any other conditions, assigns the original value of anno_v to the annotation key (anno_k).

        4. Returns the updated annotations dictionary.

        Args:
            dmge (DataModelGraphExplorer): data model graph explorer
            metadata_syn (dict): metadata used for Synapse storage
            hideBlanks (bool): if true, does not upload annotation keys with blank values.
            csv_list_regex (str): Regex to match with comma separated list
            annos (Dict[str, Any]): dictionary of annotation returned from synapse
            annotation_keys (str): display_label/class_label

        Returns:
            Dict[str, Any]: annotations as a dictionary

        ```mermaid
        flowchart TD
            A[Start] --> C{Is anno_v empty, whitespace, or NaN?}
            C -- Yes --> D{Is hide_blanks True?}
            D -- Yes --> E[Remove this annotation key from the annotation dictionary to be uploaded. Skip further processing]
            D -- No --> F[Assign empty string to annotation key]
            C -- No --> G{Is anno_v a string?}
            G -- No --> H[Assign original value of anno_v to annotation key]
            G -- Yes --> I{Does anno_v match csv_list_regex?}
            I -- Yes --> J[Get validation rule of anno_k]
            J --> K{Does the validation rule contain 'list'}
            K -- Yes --> L[Split anno_v by commas and assign as list]
            I -- No --> H
            K -- No --> H
        ```
        """
        for anno_k, anno_v in metadata_syn.items():
            # Remove keys with nan or empty string values or string that only contains white space from dict of annotations to be uploaded
            # if present on current data annotation
            if hide_blanks and (
                (isinstance(anno_v, str) and anno_v.strip() == "")
                or (isinstance(anno_v, float) and np.isnan(anno_v))
            ):
                annos["annotations"]["annotations"].pop(anno_k) if anno_k in annos[
                    "annotations"
                ]["annotations"].keys() else annos["annotations"]["annotations"]
                continue

            # Otherwise save annotation as approrpriate
            if isinstance(anno_v, float) and np.isnan(anno_v):
                annos["annotations"]["annotations"][anno_k] = ""
                continue

            # Handle strings that match the csv_list_regex and pass the validation rule
            if isinstance(anno_v, str) and re.fullmatch(csv_list_regex, anno_v):
                # Use a dictionary to dynamically choose the argument
                param = (
                    {"node_display_name": anno_k}
                    if annotation_keys == "display_label"
                    else {"node_label": anno_k}
                )
                node_validation_rules = dmge.get_node_validation_rules(**param)

                if rule_in_rule_list("list", node_validation_rules):
                    annos["annotations"]["annotations"][anno_k] = anno_v.split(",")
                    continue
            # default: assign the original value
            annos["annotations"]["annotations"][anno_k] = anno_v

        return annos

    @async_missing_entity_handler
    async def format_row_annotations(
        self,
        dmge: DataModelGraphExplorer,
        row: pd.Series,
        entityId: str,
        hideBlanks: bool,
        annotation_keys: str,
    ) -> Union[None, Dict[str, Any]]:
        """Format row annotations

        Args:
            dmge (DataModelGraphExplorer): data moodel graph explorer object
            row (pd.Series): row of the manifest
            entityId (str): entity id of the manifest
            hideBlanks (bool): when true, does not upload annotation keys with blank values. When false, upload Annotation keys with empty string values
            annotation_keys (str): display_label/class_label

        Returns:
            Union[None, Dict[str,]]: if entity id is in trash can, return None. Otherwise, return the annotations
        """
        # prepare metadata for Synapse storage (resolve display name into a name that Synapse annotations support (e.g no spaces, parenthesis)
        # note: the removal of special characters, will apply only to annotation keys; we are not altering the manifest
        # this could create a divergence between manifest column and annotations. this should be ok for most use cases.
        # columns with special characters are outside of the schema
        metadataSyn = {}
        blacklist_chars = ["(", ")", ".", " ", "-"]

        for k, v in row.to_dict().items():
            if annotation_keys == "display_label":
                keySyn = str(k).translate({ord(x): "" for x in blacklist_chars})
            elif annotation_keys == "class_label":
                keySyn = get_class_label_from_display_name(str(k)).translate(
                    {ord(x): "" for x in blacklist_chars}
                )

            # Skip `Filename` and `ETag` columns when setting annotations
            if keySyn in ["Filename", "ETag", "eTag"]:
                continue

            # truncate annotation values to 500 characters if the
            # size of values is greater than equal to 500 characters
            # add an explicit [truncatedByDataCuratorApp] message at the end
            # of every truncated message to indicate that the cell value
            # has been truncated
            if isinstance(v, str) and len(v) >= 500:
                v = v[0:472] + "[truncatedByDataCuratorApp]"

            metadataSyn[keySyn] = v

        # This will first check if the entity is already in memory, and if so, that
        # instance is used. Unfortunately, the expected return format needs to match
        # the Synapse API, so we need to convert the annotations to the expected format.
        entity = self.synapse_entity_tracker.get(
            synapse_id=entityId,
            syn=self.syn,
            download_file=False,
            retrieve_if_not_present=False,
        )
        if entity is not None:
            synapse_annotations = _convert_to_annotations_list(
                annotations=entity.annotations
            )
            annos = {
                "annotations": {
                    "id": entity.id,
                    "etag": entity.etag,
                    "annotations": synapse_annotations,
                }
            }
        else:
            annos = await self.get_async_annotation(entityId)

        # set annotation(s) for the various objects/items in a dataset on Synapse
        csv_list_regex = comma_separated_list_regex()

        annos = self.process_row_annotations(
            dmge=dmge,
            metadata_syn=metadataSyn,
            hide_blanks=hideBlanks,
            csv_list_regex=csv_list_regex,
            annos=annos,
            annotation_keys=annotation_keys,
        )

        return annos

    @missing_entity_handler
    @tracer.start_as_current_span("SynapseStorage::format_manifest_annotations")
    def format_manifest_annotations(self, manifest, manifest_synapse_id):
        """
        Set annotations for the manifest (as a whole) so they can be applied to the manifest table or csv.
        For now just getting the Component.
        """

        entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_synapse_id, syn=self.syn, download_file=False
        )
        is_file = entity.concreteType.endswith(".FileEntity")
        is_table = entity.concreteType.endswith(".TableEntity")

        if is_file:
            # Get file metadata
            metadata = self.getFileAnnotations(manifest_synapse_id)

            # If there is a defined component add it to the metadata.
            if "Component" in manifest.columns:
                # Gather component information
                component = manifest["Component"].unique()

                # Double check that only a single component is listed, else raise an error.
                try:
                    len(component) == 1
                except ValueError as err:
                    raise ValueError(
                        f"Manifest has more than one component. Please check manifest and resubmit."
                    ) from err

                # Add component to metadata
                metadata["Component"] = component[0]

        elif is_table:
            # Get table metadata
            metadata = self.getTableAnnotations(manifest_synapse_id)

        # Get annotations
        annos = OldAnnotations(
            id=entity.id, etag=entity.etag, values=entity.annotations
        )

        # Add metadata to the annotations
        for annos_k, annos_v in metadata.items():
            annos[annos_k] = annos_v

        return annos

    '''
    def annotate_upload_manifest_table(self, manifest, datasetId, metadataManifestPath,
        useSchemaLabel: bool = True, hideBlanks: bool = False, restrict_manifest = False):
        """
        Purpose:
            Works very similarly to associateMetadataWithFiles except takes in the manifest
            rather than the manifest path

        """
        
        # Add uuid for table updates and fill.
        if not "Uuid" in manifest.columns:
            manifest["Uuid"] = ''

        for idx,row in manifest.iterrows():
            if not row["Uuid"]:
                gen_uuid = uuid.uuid4()
                row["Uuid"] = gen_uuid
                manifest.loc[idx, 'Uuid'] = gen_uuid

        # add entityId as a column if not already there or
        # fill any blanks with an empty string.
        if not "entityId" in manifest.columns:
            manifest["entityId"] = ""
        else:
            manifest["entityId"].fillna("", inplace=True)

        # get a DataModelGraphExplorer object to ensure schema attribute names used in manifest are translated to schema labels for synapse annotations
        dmge = DataModelGraphExplorer()

        # Create table name here.
        if 'Component' in manifest.columns:
            table_name = manifest['Component'][0].lower() + '_synapse_storage_manifest_table'
        else:
            table_name = 'synapse_storage_manifest_table'

        # Upload manifest as a table and get the SynID and manifest
        manifest_synapse_table_id, manifest, table_manifest = self.upload_format_manifest_table(
                                                    dmge, manifest, datasetId, table_name, restrict = restrict_manifest, useSchemaLabel=useSchemaLabel,)
            
        # Iterate over manifest rows, create Synapse entities and store corresponding entity IDs in manifest if needed
        # also set metadata for each synapse entity as Synapse annotations
        for idx, row in manifest.iterrows():
            if not row["entityId"]:
                # If not using entityIds, fill with manifest_table_id so 
                row["entityId"] = manifest_synapse_table_id
                entityId = ''
            else:
                # get the entity id corresponding to this row
                entityId = row["entityId"]

        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(manifest, metadataManifestPath, datasetId, restrict_manifest)
        
        # Get annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_file_id)
        
        self.syn.set_annotations(manifest_annotations)

        logger.info("Associated manifest file with dataset on Synapse.")
        
        # Update manifest Synapse table with new entity id column.
        self.make_synapse_table(
            table_to_load = table_manifest,
            dataset_id = datasetId,
            existingTableId = manifest_synapse_table_id,
            table_name = table_name,
            update_col = 'Uuid',
            specify_schema = False,
            )
        
        # Get annotations for the table manifest
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_table_id)
        self.syn.set_annotations(manifest_annotations)
        return manifest_synapse_table_id
    '''

    def _read_manifest(self, metadataManifestPath: str) -> pd.DataFrame:
        """Helper function to read in provided manifest as a pandas DataFrame for subsequent downstream processing.
        Args:
            metadataManifestPath (str): path where manifest is stored
        Returns:
            manifest(pd.DataFrame): Manifest loaded as a pandas dataframe
        Raises:
            FileNotFoundError: Manifest file does not exist at provided path.
        """
        # read new manifest csv
        try:
            load_args = {
                "dtype": "string",
            }
            manifest = load_df(
                metadataManifestPath,
                preserve_raw_input=False,
                allow_na_values=False,
                **load_args,
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"No manifest file was found at this path: {metadataManifestPath}"
            ) from err
        return manifest

    def _add_id_columns_to_manifest(
        self, manifest: pd.DataFrame, dmge: DataModelGraphExplorer
    ):
        """Helper function to add id and entityId columns to the manifest if they do not already exist, Fill id values per row.
        Args:
            Manifest loaded as a pd.Dataframe
        Returns (pd.DataFrame):
            Manifest df with new Id and EntityId columns (and UUID values) if they were not already present.
        """

        # Add Id for table updates and fill.
        if not col_in_dataframe("Id", manifest):
            # See if schema has `Uuid` column specified
            try:
                uuid_col_in_schema = dmge.is_class_in_schema(
                    "Uuid"
                ) or dmge.is_class_in_schema("uuid")
            except KeyError:
                uuid_col_in_schema = False

            # Rename `Uuid` column if it wasn't specified in the schema
            if col_in_dataframe("Uuid", manifest) and not uuid_col_in_schema:
                manifest.rename(columns={"Uuid": "Id"}, inplace=True)
            # If no `Uuid` column exists or it is specified in the schema, create a new `Id` column
            else:
                manifest["Id"] = ""

        # Retrieve the ID column name (id, Id and ID) are treated the same.
        id_col_name = [col for col in manifest.columns if col.lower() == "id"][0]

        # Check if values have been added to the Id coulumn, if not add a UUID so value in the row is not blank.
        for idx, row in manifest.iterrows():
            if not row[id_col_name]:
                gen_uuid = str(uuid.uuid4())
                row[id_col_name] = gen_uuid
                manifest.loc[idx, id_col_name] = gen_uuid

        # add entityId as a column if not already there or
        # fill any blanks with an empty string.
        if not col_in_dataframe("entityId", manifest):
            manifest["entityId"] = ""
        else:
            manifest["entityId"].fillna("", inplace=True)

        return manifest

    def _generate_table_name(self, manifest):
        """Helper function to generate a table name for upload to synapse.

        Args:
            Manifest loaded as a pd.Dataframe

        Returns:
            table_name (str): Name of the table to load
            component_name (str): Name of the manifest component (if applicable)
        """
        # Create table name here.
        if "Component" in manifest.columns:
            component_name = manifest["Component"][0].lower()
            table_name = component_name + "_synapse_storage_manifest_table"
        else:
            component_name = ""
            table_name = "synapse_storage_manifest_table"
        return table_name, component_name

    def _create_entity_id(self, idx, row, manifest, datasetId):
        """Helper function to generate an entityId and add it to the appropriate row in the manifest.
        Args:
            row: current row of manifest being processed
            manifest (pd.DataFrame): loaded df containing user supplied data.
            datasetId (str): synapse ID of folder containing the dataset

        Returns:
            manifest (pd.DataFrame): manifest with entityId added to the appropriate row
            entityId (str): Generated Entity Id.

        """
        rowEntity = Folder(str(uuid.uuid4()), parent=datasetId)
        rowEntity = self.syn.store(rowEntity)
        entityId = rowEntity["id"]
        self.synapse_entity_tracker.add(synapse_id=entityId, entity=rowEntity)
        row["entityId"] = entityId
        manifest.loc[idx, "entityId"] = entityId
        return manifest, entityId

    async def _process_store_annos(self, requests: Set[asyncio.Task]) -> None:
        """Process annotations and store them on synapse asynchronously

        Args:
            requests (Set[asyncio.Task]): a set of tasks of formatting annotations created by format_row_annotations function in previous step

        Raises:
            RuntimeError: raise a run time error if a task failed to complete
        """
        while requests:
            done_tasks, pending_tasks = await asyncio.wait(
                requests, return_when=asyncio.FIRST_COMPLETED
            )
            requests = pending_tasks

            for completed_task in done_tasks:
                try:
                    annos = completed_task.result()

                    if isinstance(annos, Annotations):
                        logger.info(f"Successfully stored annotations for {annos.id}")
                    else:
                        # store annotations if they are not None
                        if annos:
                            entity_id = annos["annotations"]["id"]
                            logger.info(
                                f"Obtained and processed annotations for {entity_id} entity"
                            )
                            requests.add(
                                asyncio.create_task(
                                    self.store_async_annotation(annotation_dict=annos)
                                )
                            )
                except Exception as e:
                    raise RuntimeError(f"failed with { repr(e) }.") from e

    @tracer.start_as_current_span("SynapseStorage::add_annotations_to_entities_files")
    async def add_annotations_to_entities_files(
        self,
        dmge,
        manifest,
        manifest_record_type: str,
        datasetId: str,
        hideBlanks: bool,
        manifest_synapse_table_id="",
        annotation_keys: str = "class_label",
    ):
        """
        Depending on upload type add Ids to entityId row. Add anotations to connected
        files and folders. Despite the name of this function, it also applies to folders.

        Args:
            dmge: DataModelGraphExplorer Object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            datasetId (str): synapse ID of folder containing the dataset
            hideBlanks (bool): Default is false -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            manifest_synapse_table_id (str): Default is an empty string ''.
            annotation_keys: (str) display_label/class_label(default), Determines labeling syle for annotation keys. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting while ensuring the label is formatted properly for Synapse annotations.
        Returns:
            manifest (pd.DataFrame): modified to add entitiyId as appropriate

        """

        # Expected behavior is to annotate files if `Filename` is present and if file_annotations_upload is set to True regardless of `-mrt` setting
        if "filename" in [col.lower() for col in manifest.columns]:
            # get current list of files and store as dataframe
            dataset_files = self.getFilesInStorageDataset(datasetId)
            files_and_entityIds = self._get_file_entityIds(
                dataset_files=dataset_files, only_new_files=False
            )
            file_df = pd.DataFrame(files_and_entityIds)

            # Merge dataframes to add entityIds
            manifest = manifest.merge(
                file_df, how="left", on="Filename", suffixes=["_x", None]
            ).drop("entityId_x", axis=1)

        # Fill `entityId` for each row if missing and annotate entity as appropriate
        requests = set()
        for idx, row in manifest.iterrows():
            if not row["entityId"] and (
                manifest_record_type == "file_and_entities"
                or manifest_record_type == "table_file_and_entities"
            ):
                manifest, entityId = self._create_entity_id(
                    idx, row, manifest, datasetId
                )
            elif not row["entityId"] and manifest_record_type == "table_and_file":
                # If not using entityIds, fill with manifest_table_id so
                row["entityId"] = manifest_synapse_table_id
                manifest.loc[idx, "entityId"] = manifest_synapse_table_id
                entityId = ""
                # If the row is the manifest table, do not add annotations
            elif row["entityId"] == manifest_synapse_table_id:
                entityId = ""
            else:
                # get the file id of the file to annotate, collected in above step.
                entityId = row["entityId"]

            # Adding annotations to connected files.
            if entityId:
                # Format annotations for Synapse
                annos_task = asyncio.create_task(
                    self.format_row_annotations(
                        dmge, row, entityId, hideBlanks, annotation_keys
                    )
                )
                requests.add(annos_task)
        await self._process_store_annos(requests)
        return manifest

    @tracer.start_as_current_span("SynapseStorage::upload_manifest_as_table")
    def upload_manifest_as_table(
        self,
        dmge: DataModelGraphExplorer,
        manifest: pd.DataFrame,
        metadataManifestPath: str,
        datasetId: str,
        table_name: str,
        component_name: str,
        restrict: bool,
        manifest_record_type: str,
        hideBlanks: bool,
        table_manipulation: str,
        table_column_names: str,
        annotation_keys: str,
        file_annotations_upload: bool = True,
    ):
        """Upload manifest to Synapse as a table and csv.
        Args:
            dmge: DataModelGraphExplorer object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            metadataManifestPath: path to csv containing a validated metadata manifest.
            datasetId (str): synapse ID of folder containing the dataset
            table_name (str): Generated to name the table being uploaded.
            component_name (str): Name of the component manifest that is currently being uploaded.
            restrict (bool): Flag for censored data.
            manifest_record_type (str): valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            hideBlanks (bool): Default is False -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            table_malnipulation (str): Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
            table_column_names: (str): display_name/display_label/class_label (default). Sets labeling style for table column names. display_name will use the raw display name as the column name. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting.
            annotation_keys: (str) display_label/class_label (default), Sets labeling syle for annotation keys. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting while ensuring the label is formatted properly for Synapse annotations.
            file_annotations_upload (bool): Default to True. If false, do not add annotations to files.
        Return:
            manifest_synapse_file_id: SynID of manifest csv uploaded to synapse.
        """
        # Upload manifest as a table, get the ID and updated manifest.
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
            dmge=dmge,
            manifest=manifest,
            datasetId=datasetId,
            table_name=table_name,
            restrict=restrict,
            table_manipulation=table_manipulation,
            table_column_names=table_column_names,
        )

        if file_annotations_upload:
            manifest = asyncio.run(
                self.add_annotations_to_entities_files(
                    dmge,
                    manifest,
                    manifest_record_type,
                    datasetId,
                    hideBlanks,
                    manifest_synapse_table_id,
                    annotation_keys,
                )
            )
        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(
            manifest=manifest,
            metadataManifestPath=metadataManifestPath,
            datasetId=datasetId,
            restrict_manifest=restrict,
            component_name=component_name,
        )

        # Set annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(
            manifest=manifest, manifest_synapse_id=manifest_synapse_file_id
        )
        annos = self.syn.set_annotations(annotations=manifest_annotations)
        manifest_entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_synapse_file_id, syn=self.syn, download_file=False
        )
        manifest_entity.annotations = annos
        manifest_entity.etag = annos.etag

        logger.info("Associated manifest file with dataset on Synapse.")

        # Update manifest Synapse table with new entity id column.
        manifest_synapse_table_id, manifest, _ = self.uploadDB(
            dmge=dmge,
            manifest=manifest,
            datasetId=datasetId,
            table_name=table_name,
            restrict=restrict,
            table_manipulation="update",
            table_column_names=table_column_names,
        )

        # Set annotations for the table manifest
        manifest_annotations = self.format_manifest_annotations(
            manifest=manifest, manifest_synapse_id=manifest_synapse_table_id
        )
        annotations_manifest_table = self.syn.set_annotations(
            annotations=manifest_annotations
        )
        manifest_table_entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_synapse_table_id, syn=self.syn, download_file=False
        )
        manifest_table_entity.annotations = annotations_manifest_table
        manifest_table_entity.etag = annotations_manifest_table.etag

        return manifest_synapse_file_id

    @tracer.start_as_current_span("SynapseStorage::upload_manifest_as_csv")
    def upload_manifest_as_csv(
        self,
        dmge,
        manifest,
        metadataManifestPath,
        datasetId,
        restrict,
        manifest_record_type,
        hideBlanks,
        component_name,
        annotation_keys: str,
        file_annotations_upload: bool = True,
    ):
        """Upload manifest to Synapse as a csv only.
        Args:
            dmge: DataModelGraphExplorer object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            metadataManifestPath: path to csv containing a validated metadata manifest.
            datasetId (str): synapse ID of folder containing the dataset
            restrict (bool): Flag for censored data.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            hideBlanks (bool): Default is False -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            annotation_keys: (str) display_label/class_label (default), Sets labeling syle for annotation keys. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting while ensuring the label is formatted properly for Synapse annotations.
            file_annotations_upload (bool): Default to True. If false, do not add annotations to files.
        Return:
            manifest_synapse_file_id (str): SynID of manifest csv uploaded to synapse.
        """
        if file_annotations_upload:
            manifest = asyncio.run(
                self.add_annotations_to_entities_files(
                    dmge,
                    manifest,
                    manifest_record_type,
                    datasetId,
                    hideBlanks,
                    annotation_keys=annotation_keys,
                )
            )

        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(
            manifest,
            metadataManifestPath,
            datasetId,
            restrict,
            component_name=component_name,
        )

        # Set annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(
            manifest, manifest_synapse_file_id
        )
        annos = self.syn.set_annotations(manifest_annotations)
        manifest_entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_synapse_file_id, syn=self.syn, download_file=False
        )
        manifest_entity.annotations = annos
        manifest_entity.etag = annos.etag

        logger.info("Associated manifest file with dataset on Synapse.")

        return manifest_synapse_file_id

    @tracer.start_as_current_span("SynapseStorage::upload_manifest_combo")
    def upload_manifest_combo(
        self,
        dmge,
        manifest,
        metadataManifestPath,
        datasetId,
        table_name,
        component_name,
        restrict,
        manifest_record_type,
        hideBlanks,
        table_manipulation,
        table_column_names: str,
        annotation_keys: str,
        file_annotations_upload: bool = True,
    ):
        """Upload manifest to Synapse as a table and CSV with entities.
        Args:
            dmge: DataModelGraphExplorer object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            metadataManifestPath: path to csv containing a validated metadata manifest.
            datasetId (str): synapse ID of folder containing the dataset
            table_name (str): Generated to name the table being uploaded.
            component_name (str): Name of the component manifest that is currently being uploaded.
            restrict (bool): Flag for censored data.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            hideBlanks (bool): Default is False -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            table_malnipulation (str): Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
            table_column_names: (str): display_name/display_label/class_label (default). Sets labeling style for table column names. display_name will use the raw display name as the column name. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting.
            annotation_keys: (str) display_label/class_label (default), Sets labeling syle for annotation keys. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting while ensuring the label is formatted properly for Synapse annotations.
            file_annotations_upload (bool): Default to True. If false, do not add annotations to files.
        Return:
            manifest_synapse_file_id (str): SynID of manifest csv uploaded to synapse.
        """
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
            dmge=dmge,
            manifest=manifest,
            datasetId=datasetId,
            table_name=table_name,
            restrict=restrict,
            table_manipulation=table_manipulation,
            table_column_names=table_column_names,
        )

        if file_annotations_upload:
            manifest = asyncio.run(
                self.add_annotations_to_entities_files(
                    dmge,
                    manifest,
                    manifest_record_type,
                    datasetId,
                    hideBlanks,
                    manifest_synapse_table_id,
                    annotation_keys=annotation_keys,
                )
            )

        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(
            manifest, metadataManifestPath, datasetId, restrict, component_name
        )

        # Set annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(
            manifest, manifest_synapse_file_id
        )
        file_manifest_annoations = self.syn.set_annotations(manifest_annotations)
        manifest_entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_synapse_file_id, syn=self.syn, download_file=False
        )
        manifest_entity.annotations = file_manifest_annoations
        manifest_entity.etag = file_manifest_annoations.etag
        logger.info("Associated manifest file with dataset on Synapse.")

        # Update manifest Synapse table with new entity id column.
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
            dmge=dmge,
            manifest=manifest,
            datasetId=datasetId,
            table_name=table_name,
            restrict=restrict,
            table_manipulation="update",
            table_column_names=table_column_names,
        )

        # Set annotations for the table manifest
        manifest_annotations = self.format_manifest_annotations(
            manifest, manifest_synapse_table_id
        )
        table_manifest_annotations = self.syn.set_annotations(manifest_annotations)
        manifest_entity = self.synapse_entity_tracker.get(
            synapse_id=manifest_synapse_table_id, syn=self.syn, download_file=False
        )
        manifest_entity.annotations = table_manifest_annotations
        manifest_entity.etag = table_manifest_annotations.etag
        return manifest_synapse_file_id

    @tracer.start_as_current_span("SynapseStorage::associateMetadataWithFiles")
    def associateMetadataWithFiles(
        self,
        dmge: DataModelGraphExplorer,
        metadataManifestPath: str,
        datasetId: str,
        manifest_record_type: str = "table_file_and_entities",
        hideBlanks: bool = False,
        restrict_manifest=False,
        table_manipulation: str = "replace",
        table_column_names: str = "class_label",
        annotation_keys: str = "class_label",
        file_annotations_upload: bool = True,
    ) -> str:
        """Associate metadata with files in a storage dataset already on Synapse.
        Upload metadataManifest in the storage dataset folder on Synapse as well. Return synapseId of the uploaded manifest file.

        If this is a new manifest there could be no Synapse entities associated with the rows of this manifest
        this may be due to data type (e.g. clinical data) being tabular
        and not requiring files; to utilize uniform interfaces downstream
        (i.e. fileviews), a Synapse entity (a folder) is created for each row
        and an entity column is added to the manifest containing the resulting
        entity IDs; a table is also created at present as an additional interface
        for downstream query and interaction with the data.

        Args:
            dmge: DataModelGraphExplorer Object
            metadataManifestPath: path to csv containing a validated metadata manifest.
            The manifest should include a column entityId containing synapse IDs of files/entities to be associated with metadata, if that is applicable to the dataset type.
            Some datasets, e.g. clinical data, do not contain file id's, but data is stored in a table: one row per item.
            In this case, the system creates a file on Synapse for each row in the table (e.g. patient, biospecimen) and associates the columnset data as metadata/annotations to his file.
            datasetId: synapse ID of folder containing the dataset
            manifest_record_type: Default value is 'table_file_and_entities'. valid values are 'file_only', 'file_and_entities', 'table_and_file' or 'table_file_and_entities'. 'file_and_entities' will store the manifest as a csv and create Synapse files for each row in the manifest.'table_and_file' will store the manifest as a table and a csv on Synapse. 'file_only' will store the manifest as a csv only on Synapse. 'table_file_and_entities' will perform the options file_with_entites and table in combination.
            hideBlanks: Default is false. Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            restrict_manifest (bool): Default is false. Flag for censored data.
            table_malnipulation (str): Default is 'replace'. Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
            table_column_names: (str): display_name/display_label/class_label (default). Sets labeling style for table column names. display_name will use the raw display name as the column name. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting.
            annotation_keys: (str) display_label/class_label (default), Sets labeling syle for annotation keys. class_label will format the display
                name as upper camelcase, and strip blacklisted characters, display_label will strip blacklisted characters including spaces, to retain
                display label formatting while ensuring the label is formatted properly for Synapse annotations.
        Returns:
            manifest_synapse_file_id: SynID of manifest csv uploaded to synapse.
        """
        # Read new manifest CSV:
        manifest = self._read_manifest(metadataManifestPath)
        manifest = self._add_id_columns_to_manifest(manifest, dmge)

        table_name, component_name = self._generate_table_name(manifest)

        # Upload manifest to synapse based on user input (manifest_record_type)
        if manifest_record_type == "file_only":
            manifest_synapse_file_id = self.upload_manifest_as_csv(
                dmge=dmge,
                manifest=manifest,
                metadataManifestPath=metadataManifestPath,
                datasetId=datasetId,
                restrict=restrict_manifest,
                hideBlanks=hideBlanks,
                manifest_record_type=manifest_record_type,
                component_name=component_name,
                annotation_keys=annotation_keys,
                file_annotations_upload=file_annotations_upload,
            )
        elif manifest_record_type == "table_and_file":
            manifest_synapse_file_id = self.upload_manifest_as_table(
                dmge=dmge,
                manifest=manifest,
                metadataManifestPath=metadataManifestPath,
                datasetId=datasetId,
                table_name=table_name,
                component_name=component_name,
                restrict=restrict_manifest,
                hideBlanks=hideBlanks,
                manifest_record_type=manifest_record_type,
                table_manipulation=table_manipulation,
                table_column_names=table_column_names,
                annotation_keys=annotation_keys,
                file_annotations_upload=file_annotations_upload,
            )
        elif manifest_record_type == "file_and_entities":
            manifest_synapse_file_id = self.upload_manifest_as_csv(
                dmge=dmge,
                manifest=manifest,
                metadataManifestPath=metadataManifestPath,
                datasetId=datasetId,
                restrict=restrict_manifest,
                hideBlanks=hideBlanks,
                manifest_record_type=manifest_record_type,
                component_name=component_name,
                annotation_keys=annotation_keys,
                file_annotations_upload=file_annotations_upload,
            )
        elif manifest_record_type == "table_file_and_entities":
            manifest_synapse_file_id = self.upload_manifest_combo(
                dmge=dmge,
                manifest=manifest,
                metadataManifestPath=metadataManifestPath,
                datasetId=datasetId,
                table_name=table_name,
                component_name=component_name,
                restrict=restrict_manifest,
                hideBlanks=hideBlanks,
                manifest_record_type=manifest_record_type,
                table_manipulation=table_manipulation,
                table_column_names=table_column_names,
                annotation_keys=annotation_keys,
                file_annotations_upload=file_annotations_upload,
            )
        else:
            raise ValueError("Please enter a valid manifest_record_type.")
        return manifest_synapse_file_id

    def getTableAnnotations(self, table_id: str):
        """Generate dictionary of annotations for the given Synapse file.
        Synapse returns all custom annotations as lists since they
        can contain multiple values. In all cases, the values will
        be converted into strings and concatenated with ", ".

        Args:
            fileId (str): Synapse ID for dataset file.

        Returns:
            dict: Annotations as comma-separated strings.
        """
        try:
            entity = self.synapse_entity_tracker.get(
                synapse_id=table_id, syn=self.syn, download_file=False
            )
            is_table = entity.concreteType.endswith(".TableEntity")
            annotations_raw = entity.annotations
        except SynapseHTTPError:
            # If an error occurs with retrieving entity, skip it
            # This could be caused by a temporary file view that
            # was deleted since its ID was retrieved
            is_file, is_table = False, False

        # Skip anything that isn't a file or folder
        if not (is_table):
            return None

        annotations = self.getEntityAnnotations(table_id, entity, annotations_raw)

        return annotations

    def getFileAnnotations(self, fileId: str) -> Dict[str, str]:
        """Generate dictionary of annotations for the given Synapse file.
        Synapse returns all custom annotations as lists since they
        can contain multiple values. In all cases, the values will
        be converted into strings and concatenated with ", ".

        Args:
            fileId (str): Synapse ID for dataset file.

        Returns:
            dict: Annotations as comma-separated strings.
        """

        # Get entity metadata, including annotations
        try:
            entity = self.synapse_entity_tracker.get(
                synapse_id=fileId, syn=self.syn, download_file=False
            )
            is_file = entity.concreteType.endswith(".FileEntity")
            is_folder = entity.concreteType.endswith(".Folder")
            annotations_raw = entity.annotations
        except SynapseHTTPError:
            # If an error occurs with retrieving entity, skip it
            # This could be caused by a temporary file view that
            # was deleted since its ID was retrieved
            is_file, is_folder = False, False

        # Skip anything that isn't a file or folder
        if not (is_file or is_folder):
            return None

        annotations = self.getEntityAnnotations(fileId, entity, annotations_raw)

        return annotations

    def getEntityAnnotations(self, fileId, entity, annotations_raw):
        # Extract annotations from their lists and stringify. For example:
        # {'YearofBirth': [1980], 'author': ['bruno', 'milen', 'sujay']}
        annotations = dict()
        for key, vals in annotations_raw.items():
            if isinstance(vals, list) and len(vals) == 1:
                annotations[key] = str(vals[0])
            else:
                annotations[key] = ", ".join(str(v) for v in vals)

        # Add the file entity ID and eTag, which weren't lists
        assert fileId == entity.id, (
            "For some reason, the Synapse ID in the response doesn't match"
            "the Synapse ID sent in the request (via synapseclient)."
        )
        annotations["entityId"] = fileId
        annotations["eTag"] = entity.etag

        return annotations

    def getDatasetAnnotations(
        self, datasetId: str, fill_na: bool = True, force_batch: bool = False
    ) -> pd.DataFrame:
        """Generate table for annotations across all files in given dataset.

        Args:
            datasetId (str): Synapse ID for dataset folder.
            fill_na (bool): Whether to replace missing values with
                blank strings.
            force_batch (bool): Whether to force the function to use
                the batch mode, which uses a file view to retrieve
                annotations for a given dataset. Default to False
                unless there are more than 50 files in the dataset.

        Returns:
            pd.DataFrame: Table of annotations.
        """
        # Get all files in given dataset
        dataset_files = self.getFilesInStorageDataset(datasetId)

        # if there are no dataset files, there are no annotations
        # return None
        if not dataset_files:
            return pd.DataFrame()

        dataset_files_map = dict(dataset_files)
        dataset_file_ids, _ = list(zip(*dataset_files))

        # Get annotations for each file from Step 1
        # Batch mode
        try_batch = len(dataset_files) >= 50 or force_batch
        if try_batch:
            try:
                logger.info("Trying batch mode for retrieving Synapse annotations")
                table = self.getDatasetAnnotationsBatch(datasetId, dataset_file_ids)
            except (SynapseAuthenticationError, SynapseHTTPError, ValueError):
                logger.info(
                    f"Unable to create a temporary file view bound to {datasetId}. "
                    "Defaulting to slower iterative retrieval of annotations."
                )
                # Default to the slower non-batch method
                logger.info("Batch mode failed (probably due to permission error)")
                try_batch = False

        # Non-batch mode
        if not try_batch:
            logger.info("Using slower (non-batch) sequential mode")
            records = [self.getFileAnnotations(i) for i in dataset_file_ids]
            # Remove any annotations for non-file/folders (stored as None)
            records = filter(None, records)
            table = pd.DataFrame.from_records(records)

        # Add filenames for the files that "survived" annotation retrieval
        filenames = [dataset_files_map[i] for i in table["entityId"]]

        if "Filename" not in table.columns:
            table.insert(0, "Filename", filenames)

        # Ensure that entityId and eTag are at the end
        entity_ids = table.pop("entityId")
        etags = table.pop("eTag")
        table.insert(len(table.columns), "entityId", entity_ids)
        table.insert(len(table.columns), "eTag", etags)

        # Missing values are filled in with empty strings for Google Sheets
        if fill_na:
            table.fillna("", inplace=True)

        # Force all values as strings
        return table.astype(str)

    def raise_final_error(retry_state):
        return retry_state.outcome.result()

    def checkIfinAssetView(self, syn_id) -> str:
        # get data in administrative fileview for this pipeline
        assetViewTable = self.getStorageFileviewTable()
        all_files = list(assetViewTable["id"])
        if syn_id in all_files:
            return True
        else:
            return False

    @tracer.start_as_current_span("SynapseStorage::getDatasetProject")
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_chain(
            *[wait_fixed(10) for i in range(2)]
            + [wait_fixed(15) for i in range(2)]
            + [wait_fixed(20)]
        ),
        retry=retry_if_exception_type(LookupError),
        retry_error_callback=raise_final_error,
    )
    def getDatasetProject(self, datasetId: str) -> str:
        """Get parent project for a given dataset ID.

        Args:
            datasetId (str): Synapse entity ID (folder or project).

        Raises:
            ValueError: Raised if Synapse ID cannot be retrieved
            by the user or if it doesn't appear in the file view.

        Returns:
            str: The Synapse ID for the parent project.
        """

        # Subset main file view
        dataset_index = self.storageFileviewTable["id"] == datasetId
        dataset_row = self.storageFileviewTable[dataset_index]

        # re-query if no datasets found
        if dataset_row.empty:
            sleep(5)
            self.query_fileview(force_requery=True)
            # Subset main file view
            dataset_index = self.storageFileviewTable["id"] == datasetId
            dataset_row = self.storageFileviewTable[dataset_index]

        # Return `projectId` for given row if only one found
        if len(dataset_row) == 1:
            dataset_project = dataset_row["projectId"].values[0]
            return dataset_project

        # Otherwise, check if already project itself
        try:
            syn_object = self.synapse_entity_tracker.get(
                synapse_id=datasetId, syn=self.syn, download_file=False
            )
            if syn_object.properties["concreteType"].endswith("Project"):
                return datasetId
        except SynapseHTTPError:
            raise PermissionError(
                f"The given dataset ({datasetId}) isn't accessible with this "
                "user. This might be caused by a typo in the dataset Synapse ID."
            )

        # If not, then assume dataset not in file view
        raise LookupError(
            f"The given dataset ({datasetId}) doesn't appear in the "
            f"configured file view ({self.storageFileview}). This might "
            "mean that the file view's scope needs to be updated."
        )

    def getDatasetAnnotationsBatch(
        self, datasetId: str, dataset_file_ids: Sequence[str] = None
    ) -> pd.DataFrame:
        """Generate table for annotations across all files in given dataset.
        This function uses a temporary file view to generate a table
        instead of iteratively querying for individual entity annotations.
        This function is expected to run much faster than
        `self.getDatasetAnnotationsBatch` on large datasets.

        Args:
            datasetId (str): Synapse ID for dataset folder.
            dataset_file_ids (Sequence[str]): List of Synapse IDs
                for dataset files/folders used to subset the table.

        Returns:
            pd.DataFrame: Table of annotations.
        """
        # Create data frame from annotations file view
        with DatasetFileView(datasetId, self.syn) as fileview:
            table = fileview.query()

        if dataset_file_ids:
            table = table.loc[table.index.intersection(dataset_file_ids)]

        table = table.reset_index(drop=True)

        return table

    def _get_table_schema_by_cname(self, table_schema):
        # assume no duplicate column names in the table
        table_schema_by_cname = {}

        for col_record in table_schema:
            # TODO clean up dictionary for compactness (e.g. remove redundant 'name' key)
            table_schema_by_cname[col_record["name"]] = col_record

        return table_schema_by_cname


class TableOperations:
    """
    Object to hold functions for various table operations specific to the Synapse Asset Store.

    Currently implement operations are:
    createTable: upload a manifest as a new table when none exist
    replaceTable: replace a metadata in a table from one manifest with metadata from another manifest
    updateTable: add a column to a table that already exists on synapse

    Operations currently in development are:
    upsertTable: add metadata from a manifest to an existing table that contains metadata from another manifest
    """

    def __init__(
        self,
        synStore: SynapseStorage,
        tableToLoad: pd.DataFrame = None,
        tableName: str = None,
        datasetId: str = None,
        existingTableId: str = None,
        restrict: bool = False,
        synapse_entity_tracker: SynapseEntityTracker = None,
    ):
        """
        Class governing table operations (creation, replacement, upserts, updates) in schematic

        tableToLoad: manifest formatted appropriately for the table
        tableName: name of the table to be uploaded
        datasetId: synID of the dataset for the manifest
        existingTableId: synId of the table currently exising on synapse (if there is one)
        restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions
        synapse_entity_tracker: Tracker for a pull-through cache of Synapse entities

        """
        self.synStore = synStore
        self.tableToLoad = tableToLoad
        self.tableName = tableName
        self.datasetId = datasetId
        self.existingTableId = existingTableId
        self.restrict = restrict
        self.synapse_entity_tracker = synapse_entity_tracker or SynapseEntityTracker()

    @tracer.start_as_current_span("TableOperations::createTable")
    def createTable(
        self,
        columnTypeDict: dict = None,
        specifySchema: bool = True,
    ):
        """
        Method to create a table from a metadata manifest and upload it to synapse

        Args:
            columnTypeDict: dictionary schema for table columns: type, size, etc
            specifySchema: to specify a specific schema for the table format

        Returns:
            table.schema.id: synID of the newly created table
        """
        datasetEntity = self.synapse_entity_tracker.get(
            synapse_id=self.datasetId, syn=self.synStore.syn, download_file=False
        )
        datasetName = datasetEntity.name
        table_schema_by_cname = self.synStore._get_table_schema_by_cname(columnTypeDict)

        if not self.tableName:
            self.tableName = datasetName + "table"
        datasetParentProject = self.synStore.getDatasetProject(self.datasetId)
        if specifySchema:
            if columnTypeDict == {}:
                logger.error("Did not provide a columnTypeDict.")
            # create list of columns:
            cols = []
            for col in self.tableToLoad.columns:
                if col in table_schema_by_cname:
                    col_type = table_schema_by_cname[col]["columnType"]
                    max_size = (
                        table_schema_by_cname[col]["maximumSize"]
                        if "maximumSize" in table_schema_by_cname[col].keys()
                        else 100
                    )
                    max_list_len = 250
                    if max_size and max_list_len:
                        cols.append(
                            Column(
                                name=col,
                                columnType=col_type,
                                maximumSize=max_size,
                                maximumListLength=max_list_len,
                            )
                        )
                    elif max_size:
                        cols.append(
                            Column(name=col, columnType=col_type, maximumSize=max_size)
                        )
                    else:
                        cols.append(Column(name=col, columnType=col_type))
                else:
                    # TODO add warning that the given col was not found and it's max size is set to 100
                    cols.append(Column(name=col, columnType="STRING", maximumSize=100))
            schema = Schema(
                name=self.tableName, columns=cols, parent=datasetParentProject
            )
            table = Table(schema, self.tableToLoad)
            table = self.synStore.syn.store(table, isRestricted=self.restrict)
            # Commented out until https://sagebionetworks.jira.com/browse/PLFM-8605 is resolved
            # self.synapse_entity_tracker.add(synapse_id=table.schema.id, entity=table.schema)
            self.synapse_entity_tracker.remove(synapse_id=table.schema.id)
            return table.schema.id
        else:
            # For just uploading the tables to synapse using default
            # column types.
            table = build_table(self.tableName, datasetParentProject, self.tableToLoad)
            table = self.synStore.syn.store(table, isRestricted=self.restrict)
            # Commented out until https://sagebionetworks.jira.com/browse/PLFM-8605 is resolved
            # self.synapse_entity_tracker.add(synapse_id=table.schema.id, entity=table.schema)
            self.synapse_entity_tracker.remove(synapse_id=table.schema.id)
            return table.schema.id

    @tracer.start_as_current_span("TableOperations::replaceTable")
    def replaceTable(
        self,
        specifySchema: bool = True,
        columnTypeDict: dict = None,
    ):
        """
        Method to replace an existing table on synapse with metadata from a new manifest

        Args:
            specifySchema: to infer a schema for the table format
            columnTypeDict: dictionary schema for table columns: type, size, etc

        Returns:
           existingTableId: synID of the already existing table that had its metadata replaced
        """
        datasetEntity = self.synapse_entity_tracker.get(
            synapse_id=self.datasetId, syn=self.synStore.syn, download_file=False
        )

        datasetName = datasetEntity.name
        table_schema_by_cname = self.synStore._get_table_schema_by_cname(columnTypeDict)
        existing_table, existing_results = self.synStore.get_synapse_table(
            self.existingTableId
        )
        # remove rows
        self.synStore.syn.delete(existing_results)
        # Data changes such as removing all rows causes the eTag to change.
        self.synapse_entity_tracker.remove(synapse_id=self.existingTableId)
        # wait for row deletion to finish on synapse before getting empty table
        sleep(10)

        # removes all current columns
        current_table = self.synapse_entity_tracker.get(
            synapse_id=self.existingTableId, syn=self.synStore.syn, download_file=False
        )

        current_columns = self.synStore.syn.getTableColumns(current_table)
        for col in current_columns:
            current_table.removeColumn(col)

        if not self.tableName:
            self.tableName = datasetName + "table"

        # Process columns according to manifest entries
        table_schema_by_cname = self.synStore._get_table_schema_by_cname(columnTypeDict)
        datasetParentProject = self.synStore.getDatasetProject(self.datasetId)
        if specifySchema:
            if columnTypeDict == {}:
                logger.error("Did not provide a columnTypeDict.")
            # create list of columns:
            cols = []

            for col in self.tableToLoad.columns:
                if col in table_schema_by_cname:
                    col_type = table_schema_by_cname[col]["columnType"]
                    max_size = (
                        table_schema_by_cname[col]["maximumSize"]
                        if "maximumSize" in table_schema_by_cname[col].keys()
                        else 100
                    )
                    max_list_len = 250
                    if max_size and max_list_len:
                        cols.append(
                            Column(
                                name=col,
                                columnType=col_type,
                                maximumSize=max_size,
                                maximumListLength=max_list_len,
                            )
                        )
                    elif max_size:
                        cols.append(
                            Column(name=col, columnType=col_type, maximumSize=max_size)
                        )
                    else:
                        cols.append(Column(name=col, columnType=col_type))
                else:
                    # TODO add warning that the given col was not found and it's max size is set to 100
                    cols.append(Column(name=col, columnType="STRING", maximumSize=100))

            # adds new columns to schema
            for col in cols:
                current_table.addColumn(col)
            table_result = self.synStore.syn.store(
                current_table, isRestricted=self.restrict
            )
            # Commented out until https://sagebionetworks.jira.com/browse/PLFM-8605 is resolved
            # self.synapse_entity_tracker.add(synapse_id=table_result.schema.id, entity=table_result.schema)
            self.synapse_entity_tracker.remove(synapse_id=table_result.id)

            # wait for synapse store to finish
            sleep(1)

            # build schema and table from columns and store with necessary restrictions
            schema = Schema(
                name=self.tableName, columns=cols, parent=datasetParentProject
            )
            schema.id = self.existingTableId
            table = Table(schema, self.tableToLoad, etag=existing_results.etag)
            table = self.synStore.syn.store(table, isRestricted=self.restrict)
            # Commented out until https://sagebionetworks.jira.com/browse/PLFM-8605 is resolved
            # self.synapse_entity_tracker.add(synapse_id=table.schema.id, entity=table.schema)
            self.synapse_entity_tracker.remove(synapse_id=table.schema.id)
        else:
            logging.error("Must specify a schema for table replacements")

        # remove system metadata from manifest
        existing_table.drop(columns=["ROW_ID", "ROW_VERSION"], inplace=True)
        return self.existingTableId

    @tracer.start_as_current_span("TableOperations::_get_auth_token")
    def _get_auth_token(
        self,
    ):
        authtoken = None

        # Get access token from environment variable if available
        # Primarily useful for testing environments, with other possible usefulness for containers
        env_access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
        if env_access_token:
            authtoken = env_access_token
            return authtoken

        # Get token from authorization header
        # Primarily useful for API endpoint functionality
        if "Authorization" in self.synStore.syn.default_headers:
            authtoken = self.synStore.syn.default_headers["Authorization"].split(
                "Bearer "
            )[-1]
            return authtoken

        # retrive credentials from synapse object
        # Primarily useful for local users, could only be stored here when a .synapseConfig file is used, but including to be safe
        synapse_object_creds = self.synStore.syn.credentials
        if hasattr(synapse_object_creds, "_token"):
            authtoken = synapse_object_creds.secret

        # Try getting creds from .synapseConfig file if it exists
        # Primarily useful for local users. Seems to correlate with credentials stored in synaspe object when logged in
        if os.path.exists(CONFIG.synapse_configuration_path):
            config = get_config_file(CONFIG.synapse_configuration_path)

            # check which credentials are provided in file
            if config.has_option("authentication", "authtoken"):
                authtoken = config.get("authentication", "authtoken")

        # raise error if required credentials are not found
        if not authtoken:
            raise NameError(
                "authtoken credentials could not be found in the environment, synapse object, or the .synapseConfig file"
            )

        return authtoken

    @tracer.start_as_current_span("TableOperations::upsertTable")
    def upsertTable(self, dmge: DataModelGraphExplorer):
        """
        Method to upsert rows from a new manifest into an existing table on synapse
        For upsert functionality to work, primary keys must follow the naming convention of <componenet>_id
        `-tm upsert` should be used for initial table uploads if users intend to upsert into them at a later time; using 'upsert' at creation will generate the metadata necessary for upsert functionality.
        Currently it is required to use -dl/--use_display_label with table upserts.


        Args:
            dmge: DataModelGraphExplorer instance

        Returns:
           existingTableId: synID of the already existing table that had its metadata replaced
        """

        authtoken = self._get_auth_token()

        synapseDB = SynapseDatabase(
            auth_token=authtoken,
            project_id=self.synStore.getDatasetProject(self.datasetId),
            syn=self.synStore.syn,
            synapse_entity_tracker=self.synapse_entity_tracker,
        )

        try:
            # Try performing upsert
            synapseDB.upsert_table_rows(
                table_name=self.tableName, data=self.tableToLoad
            )
        except SynapseHTTPError as ex:
            # If error is raised because Table has old `Uuid` column and not new `Id` column, then handle and re-attempt upload
            if "Id is not a valid column name or id" in str(ex):
                self._update_table_uuid_column(dmge)
                synapseDB.upsert_table_rows(
                    table_name=self.tableName, data=self.tableToLoad
                )
            # Raise if other error
            else:
                raise ex

        return self.existingTableId

    @tracer.start_as_current_span("TableOperations::_update_table_uuid_column")
    def _update_table_uuid_column(
        self,
        dmge: DataModelGraphExplorer,
    ) -> None:
        """Removes the `Uuid` column when present, and relpaces with an `Id` column
        Used to enable backwards compatability for manifests using the old `Uuid` convention

        Args:
            dmge: DataModelGraphExplorer instance

        Returns:
            None
        """

        # Get the columns of the schema
        schema = self.synapse_entity_tracker.get(
            synapse_id=self.existingTableId, syn=self.synStore.syn, download_file=False
        )

        cols = self.synStore.syn.getTableColumns(schema)

        # Iterate through columns until `Uuid` column is found
        for col in cols:
            if col.name.lower() == "uuid":
                # See if schema has `Uuid` column specified
                try:
                    uuid_col_in_schema = dmge.is_class_in_schema(col.name)
                except KeyError:
                    uuid_col_in_schema = False

                # If there is, then create a new `Id` column from scratch
                if uuid_col_in_schema:
                    new_col = Column(columnType="STRING", maximumSize=64, name="Id")
                    schema.addColumn(new_col)
                    schema = self.synStore.syn.store(schema)
                    # self.synapse_entity_tracker.add(synapse_id=schema.id, entity=schema)
                    # Commented out until https://sagebionetworks.jira.com/browse/PLFM-8605 is resolved
                    self.synapse_entity_tracker.remove(synapse_id=schema.id)
                # If there is not, then use the old `Uuid` column as a basis for the new `Id` column
                else:
                    # Build ColumnModel that will be used for new column
                    id_column = Column(
                        name="Id",
                        columnType="STRING",
                        maximumSize=64,
                        defaultValue=None,
                        maximumListLength=1,
                    )
                    new_col_response = self.synStore.syn.store(id_column)

                    # Define columnChange body
                    columnChangeDict = {
                        "concreteType": "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest",
                        "entityId": self.existingTableId,
                        "changes": [
                            {
                                "oldColumnId": col["id"],
                                "newColumnId": new_col_response["id"],
                            }
                        ],
                    }

                    self.synStore.syn._async_table_update(
                        table=self.existingTableId,
                        changes=[columnChangeDict],
                        wait=False,
                    )
                break

        return

    @tracer.start_as_current_span("TableOperations::updateTable")
    def updateTable(
        self,
        update_col: str = "Id",
    ):
        """
        Method to update an existing table with a new column

        Args:
            updateCol: column to index the old and new tables on

        Returns:
           existingTableId: synID of the already existing table that had its metadata replaced
        """
        existing_table, existing_results = self.synStore.get_synapse_table(
            self.existingTableId
        )

        self.tableToLoad = update_df(existing_table, self.tableToLoad, update_col)
        # store table with existing etag data and impose restrictions as appropriate
        table_result = self.synStore.syn.store(
            Table(self.existingTableId, self.tableToLoad, etag=existing_results.etag),
            isRestricted=self.restrict,
        )
        # We cannot store the Table to the `synapse_entity_tracker` because there is
        # not `Schema` on the table object. The above `.store()` function call would
        # also update the ETag of the entity within Synapse. Remove it from the tracker
        # and re-retrieve it later on if needed again.
        self.synapse_entity_tracker.remove(synapse_id=table_result.tableId)

        return self.existingTableId


class DatasetFileView:
    """Helper class to create temporary dataset file views.
    This class can be used in conjunction with a 'with' statement.
    This will ensure that the file view is deleted automatically.
    See SynapseStorage.getDatasetAnnotationsBatch for example usage.
    """

    def __init__(
        self,
        datasetId: str,
        synapse: Synapse,
        name: str = None,
        temporary: bool = True,
        parentId: str = None,
    ) -> None:
        """Create a file view scoped to a dataset folder.

        Args:
            datasetId (str): Synapse ID for a dataset folder/project.
            synapse (Synapse): Used for Synapse requests.
            name (str): Name of the file view (temporary or not).
            temporary (bool): Whether to delete the file view on exit
                of either a 'with' statement or Python entirely.
            parentId (str, optional): Synapse ID specifying where to
                store the file view. Defaults to datasetId.
        """

        self.datasetId = datasetId
        self.synapse = synapse
        self.is_temporary = temporary

        if name is None:
            self.name = f"schematic annotation file view for {self.datasetId}"

        if self.is_temporary:
            uid = secrets.token_urlsafe(5)
            self.name = f"{self.name} - UID {uid}"

        # TODO: Allow a DCC admin to configure a "universal parent"
        #       Such as a Synapse project writeable by everyone.
        self.parentId = datasetId if parentId is None else parentId

        # TODO: Create local sharing setting to hide from everyone else
        view_schema = EntityViewSchema(
            name=self.name,
            parent=self.parentId,
            scopes=self.datasetId,
            includeEntityTypes=[EntityViewType.FILE, EntityViewType.FOLDER],
            addDefaultViewColumns=False,
            addAnnotationColumns=True,
        )

        # TODO: Handle failure due to insufficient permissions by
        #       creating a temporary new project to store view
        self.view_schema = self.synapse.store(view_schema)

        # These are filled in after calling `self.query()`
        self.results = None
        self.table = None

        # Ensure deletion of the file view (last resort)
        if self.is_temporary:
            atexit.register(self.delete)

    def __enter__(self):
        """Return file view when entering 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Delete file view when exiting 'with' statement."""
        if self.is_temporary:
            self.delete()

    def delete(self):
        """Delete the file view on Synapse without deleting local table."""
        if self.view_schema is not None:
            self.synapse.delete(self.view_schema)
            self.view_schema = None

    def query(self, tidy=True, force=False):
        """Retrieve file view as a data frame (raw format sans index)."""
        if self.table is None or force:
            fileview_id = self.view_schema["id"]
            self.results = self.synapse.tableQuery(f"select * from {fileview_id}")
            self.table = self.results.asDataFrame(
                rowIdAndVersionInIndex=False,
                na_values=STR_NA_VALUES_FILTERED,
                keep_default_na=False,
            )
        if tidy:
            self.tidy_table()
        return self.table

    def tidy_table(self):
        """Convert raw file view data frame into more usable format."""
        assert self.table is not None, "Must call `self.query()` first."
        self._fix_default_columns()
        self._fix_list_columns()
        self._fix_int_columns()
        return self.table

    def _fix_default_columns(self):
        """Rename default columns to match schematic expectations."""

        # Drop ROW_VERSION column if present
        if "ROW_VERSION" in self.table:
            del self.table["ROW_VERSION"]

        # Rename id column to entityId and set as data frame index
        if "ROW_ID" in self.table:
            self.table["entityId"] = "syn" + self.table["ROW_ID"].astype(str)
            self.table = self.table.set_index("entityId", drop=False)
            del self.table["ROW_ID"]

        # Rename ROW_ETAG column to eTag and place at end of data frame
        if "ROW_ETAG" in self.table:
            row_etags = self.table.pop("ROW_ETAG")

            # eTag column may already present if users annotated data without submitting manifest
            # we're only concerned with the new values and not the existing ones
            if "eTag" in self.table:
                del self.table["eTag"]

            self.table.insert(len(self.table.columns), "eTag", row_etags)

        return self.table

    def _get_columns_of_type(self, types):
        """Helper function to get list of columns of a given type(s)."""
        matching_columns = []
        for header in self.results.headers:
            if header.columnType in types:
                matching_columns.append(header.name)
        return matching_columns

    def _fix_list_columns(self):
        """Fix formatting of list-columns."""
        list_types = {"STRING_LIST", "INTEGER_LIST", "BOOLEAN_LIST"}
        list_columns = self._get_columns_of_type(list_types)
        for col in list_columns:
            self.table[col] = self.table[col].apply(lambda x: ", ".join(x))
        return self.table

    def _fix_int_columns(self):
        """Ensure that integer-columns are actually integers."""
        int_columns = self._get_columns_of_type({"INTEGER"})
        for col in int_columns:
            # Coercing to string because NaN is a floating point value
            # and cannot exist alongside integers in a column
            def to_int_fn(x):
                return "" if np.isnan(x) else str(int(x))

            self.table[col] = self.table[col].apply(to_int_fn)
        return self.table
