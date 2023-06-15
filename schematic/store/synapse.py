from datetime import datetime, timedelta
from copy import deepcopy
import os
import uuid  # used to generate unique names for entities
import json
import atexit
import logging
import secrets
from dataclasses import dataclass
import tempfile

# allows specifying explicit variable types
from typing import Dict, List, Tuple, Sequence, Union
from collections import OrderedDict
from tenacity import retry, stop_after_attempt, wait_chain, wait_fixed, retry_if_exception_type

import numpy as np
import pandas as pd
import re
import synapseclient
from time import sleep
from synapseclient import (
    Synapse,
    File,
    Folder,
    Table,
    Schema,
    EntityViewSchema,
    EntityViewType,
    Column,
    as_table_columns,
)

from synapseclient.entity import File
from synapseclient.table import CsvFileTable, build_table, Schema
from synapseclient.annotations import from_synapse_annotations
from synapseclient.core.exceptions import SynapseHTTPError, SynapseAuthenticationError, SynapseUnmetAccessRestrictions
from synapseutils import walk
from synapseutils.copy_functions import changeFileMetaData

import uuid

from schematic_db.synapse.synapse import SynapseConfig
from schematic_db.rdb.synapse_database import SynapseDatabase


from schematic.utils.df_utils import update_df, load_df, col_in_dataframe, populate_df_col_with_another_col
from schematic.utils.validate_utils import comma_separated_list_regex, rule_in_rule_list
from schematic.utils.general import entity_type_mapping, get_dir_size, convert_size, convert_gb_to_bytes, create_temp_folder
from schematic.schemas.explorer import SchemaExplorer
from schematic.schemas.generator import SchemaGenerator
from schematic.store.base import BaseStorage
from schematic.exceptions import MissingConfigValueError, AccessCredentialsError

from schematic import CONFIG

logger = logging.getLogger("Synapse storage")

@dataclass
class ManifestDownload(object):
    """
    syn: an object of type synapseclient.
    manifest_id: id of a manifest  
    """
    syn: synapseclient.Synapse
    manifest_id: str

    def _download_manifest_to_folder(self) -> File:
        """
        try downloading a manifest to local cache or a given folder
        manifest
        Return: 
            manifest_data: A Synapse file entity of the downloaded manifest
        """
        # TO DO: potentially deprecate the if else statement because "manifest_folder" key always exist in config (See issue FDS-349 in Jira)
        # on AWS, to avoid overriding manifest, we download the manifest to a temporary folder
        if "SECRETS_MANAGER_SECRETS" in os.environ:
            temporary_manifest_storage = "/var/tmp/temp_manifest_download"
            if not os.path.exists(temporary_manifest_storage):
                os.mkdir("/var/tmp/temp_manifest_download")
            download_location = create_temp_folder(temporary_manifest_storage)

        elif CONFIG["synapse"]["manifest_folder"]:
            download_location=CONFIG["synapse"]["manifest_folder"]

        else:
            download_location=None
        
        if not download_location:
            manifest_data = self.syn.get(
                        self.manifest_id,
                    )
        # if download_location is provided and it is not an empty string
        else:
            manifest_data = self.syn.get(
                    self.manifest_id,
                    downloadLocation=download_location,
                    ifcollision="overwrite.local",
                )
        return manifest_data 

    def _entity_type_checking(self) -> str:
        """
        check the entity type of the id that needs to be downloaded
        Return: 
             if the entity type is wrong, raise an error
        """
        # check the type of entity
        entity_type = entity_type_mapping(self.syn, self.manifest_id)
        if entity_type  != "file":
            logger.error(f'You are using entity type: {entity_type}. Please provide a file ID')

    @staticmethod
    def download_manifest(self, newManifestName: str="", manifest_df: pd.DataFrame=pd.DataFrame()) -> Union[str,File]:
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
            manifest_data = self._download_manifest_to_folder()
        except(SynapseUnmetAccessRestrictions, SynapseAuthenticationError):
            # if there's an error getting an uncensored manifest, try getting the censored manifest
            if not manifest_df.empty:
                censored_regex=re.compile('.*censored.*')
                censored = manifest_df['name'].str.contains(censored_regex)
                new_manifest_id=manifest_df[censored]["id"][0]
                self.manifest_id = new_manifest_id
                try: 
                    manifest_data = self._download_manifest_to_folder()
                except (SynapseUnmetAccessRestrictions, SynapseAuthenticationError) as e:
                    raise PermissionError("You don't have access to censored and uncensored manifests in this dataset.") from e
            else:
                logger.error(f"You don't have access to the requested resource: {self.manifest_id}")

        if newManifestName and os.path.exists(manifest_data.get('path')):
            # Rename the file we just made to the new name
            new_manifest_filename = newManifestName + '.csv'

            # get location of existing manifest. The manifest that will be renamed should live in the same folder as existing manifest.
            parent_folder = os.path.dirname(manifest_data.get('path'))

            new_manifest_path_name = os.path.join(parent_folder, new_manifest_filename)
            os.rename(manifest_data['path'], new_manifest_path_name)

            # Update file names/paths in manifest_data
            manifest_data['name'] = new_manifest_filename
            manifest_data['filename'] = new_manifest_filename
            manifest_data['path'] = new_manifest_path_name
        return manifest_data

class SynapseStorage(BaseStorage):
    """Implementation of Storage interface for datasets/files stored on Synapse.
    Provides utilities to list files in a specific project; update files annotations, create fileviews, etc.

    TODO: Need to define the interface and rename and/or refactor some of the methods below.
    """

    def __init__(
        self,
        token: str = None,  # optional parameter retrieved from browser cookie
        access_token: str = None,
        project_scope: List = None,
    ) -> None:
        """Initializes a SynapseStorage object.
        Args:
            syn: an object of type synapseclient.
            token: optional token parameter (typically a 'str') as found in browser cookie upon login to synapse.
            access_token: optional access token (personal or oauth)
            TODO: move away from specific project setup and work with an interface that Synapse specifies (e.g. based on schemas).
        Exceptions:
            KeyError: when the 'storage' config object is missing values for essential keys.
            AttributeError: when the 'storageFileview' attribute (of class SynapseStorage) does not have a value associated with it.
            synapseclient.core.exceptions.SynapseHTTPError: check if the current user has permission to access the Synapse entity.
            ValueError: when Admin fileview cannot be found (describe further).
        Typical usage example:
            syn_store = SynapseStorage()
        """

        self.syn = self.login(token, access_token)
        self.project_scope = project_scope


        # check if "master_fileview" has been set
        try: 
            self.storageFileview = CONFIG["synapse"]["master_fileview"]
        except KeyError: 
            raise MissingConfigValueError(("synapse", "master_fileview"))

        # check if "manifest_basename" has been set
        try: 
            self.manifest = CONFIG["synapse"]["manifest_basename"]
        except KeyError: 
            raise MissingConfigValueError(("synapse", "manifest_basename"))

        self._query_fileview()

    def _purge_synapse_cache(self, root_dir: str = "/var/www/.synapseCache/", maximum_storage_allowed_cache_gb=7):
        """
        Purge synapse cache if it exceeds 7GB
        Args:
            root_dir: directory of the .synapseCache function
            maximum_storage_allowed_cache_gb: the maximum storage allowed before purging cache. Default is 7 GB. 

        Returns: 
            if size of cache reaches a certain threshold (default is 7GB), return the number of files that get deleted
            otherwise, return the total remaining space (assuming total ephemeral storage is 20GB on AWS )
        """
        # try clearing the cache
        # scan a directory and check size of files
        cache = self.syn.cache
        if os.path.exists(root_dir):
            maximum_storage_allowed_cache_bytes = convert_gb_to_bytes(maximum_storage_allowed_cache_gb)
            total_ephemeral_storag_gb = 20
            total_ephemeral_storage_bytes = convert_gb_to_bytes(total_ephemeral_storag_gb)
            nbytes = get_dir_size(root_dir)
            # if 7 GB has already been taken, purge cache before 15 min
            if nbytes >= maximum_storage_allowed_cache_bytes:
                minutes_earlier = datetime.strftime(datetime.utcnow()- timedelta(minutes = 15), '%s')
                num_of_deleted_files = cache.purge(before_date = int(minutes_earlier))
                logger.info(f'{num_of_deleted_files} number of files have been deleted from {root_dir}')
            else:
                remaining_space = total_ephemeral_storage_bytes - nbytes
                converted_space = convert_size(remaining_space)
                logger.info(f'Estimated {remaining_space} bytes (which is approximately {converted_space}) remained in ephemeral storage after calculating size of .synapseCache excluding OS')

    def _query_fileview(self):
        self._purge_synapse_cache()
        try:
            self.storageFileview = CONFIG["synapse"]["master_fileview"]
            self.manifest = CONFIG["synapse"]["manifest_basename"]
            if self.project_scope:
                self.storageFileviewTable = self.syn.tableQuery(
                    f"SELECT * FROM {self.storageFileview} WHERE projectId IN {tuple(self.project_scope + [''])}"
                    ).asDataFrame()
            else:
                # get data in administrative fileview for this pipeline
                self.storageFileviewTable = self.syn.tableQuery(
                    "SELECT * FROM " + self.storageFileview
                ).asDataFrame()

        except AttributeError:
            raise AttributeError("storageFileview attribute has not been set.")
        except SynapseHTTPError:
            raise AccessCredentialsError(self.storageFileview)    

    @staticmethod
    def login(token=None, access_token=None):
        # If no token is provided, try retrieving access token from environment
        if not token and not access_token:
            access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")

        # login using a token
        if token:
            syn = synapseclient.Synapse()

            try:
                syn.login(sessionToken=token, silent=True)
            except synapseclient.core.exceptions.SynapseHTTPError:
                raise ValueError("Please make sure you are logged into synapse.org.")
        elif access_token:
            try:
                syn = synapseclient.Synapse()
                syn.default_headers["Authorization"] = f"Bearer {access_token}"
            except synapseclient.core.exceptions.SynapseHTTPError:
                raise ValueError("No access to resources. Please make sure that your token is correct")
        else:
            # login using synapse credentials provided by user in .synapseConfig (default) file
            syn = synapseclient.Synapse(configPath=CONFIG.SYNAPSE_CONFIG_PATH)
            syn.login(silent=True)
            
        return syn

    def missing_entity_handler(method):
        def wrapper(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except(SynapseHTTPError) as ex:
                str_message = str(ex).replace("\n","")
                if 'trash' in str_message or 'does not exist' in str_message:
                    logging.warning(str_message)
                    return None
                else:
                    raise ex
        return wrapper


    def getStorageFileviewTable(self):
        """ Returns the storageFileviewTable obtained during initialization.
        """
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

    def getStorageProjects(self, project_scope: List = None) -> List[str]:
        """Gets all storage projects the current user has access to, within the scope of the 'storageFileview' attribute.

        Returns:
            A list of storage projects the current user has access to; the list consists of tuples (projectId, projectName).
        """

        # get the set of all storage Synapse project accessible for this pipeline
        storageProjects = self.storageFileviewTable["projectId"].unique()

        # get the set of storage Synapse project accessible for this user

        # get current user name and user ID
        currentUser = self.syn.getUserProfile()
        currentUserName = currentUser.userName
        currentUserId = currentUser.ownerId

        # get a list of projects from Synapse
        currentUserProjects = self.getPaginatedRestResults(currentUserId)

        # prune results json filtering project id
        currentUserProjects = [
            currentUserProject.get("id")
            for currentUserProject in currentUserProjects["results"]
        ]

        # find set of user projects that are also in this pipeline's storage projects set
        storageProjects = list(set(storageProjects) & set(currentUserProjects))
        
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
            projectName = self.syn.get(projectId, downloadFile=False).name
            projects.append((projectId, projectName))

        sorted_projects_list = sorted(projects, key=lambda tup: tup[0])

        return sorted_projects_list

    def getStorageDatasetsInProject(self, projectId: str) -> List[str]:
        """Gets all datasets in folder under a given storage project that the current user has access to.

        Args:
            projectId: synapse ID of a storage project.

        Returns:
            A list of datasets within the given storage project; the list consists of tuples (datasetId, datasetName).
            None: If the projectId cannot be found on Synapse.
        """

        # select all folders and fetch their names from within the storage project;
        # if folder content type is defined, only select folders that contain datasets
        areDatasets = False
        if "contentType" in self.storageFileviewTable.columns:
            foldersTable = self.storageFileviewTable[
                (self.storageFileviewTable["contentType"] == "dataset")
                & (self.storageFileviewTable["projectId"] == projectId)
            ]
            areDatasets = True
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

    def getFilesInStorageDataset(
        self, datasetId: str, fileNames: List = None, fullpath: bool = True
    ) -> List[Tuple[str, str]]:
        """Gets all files in a given dataset folder.

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

        # select all files within a given storage dataset folder (top level folder in a Synapse storage project or folder marked with contentType = 'dataset')
        walked_path = walk(self.syn, datasetId)

        file_list = []

        # iterate over all results
        for dirpath, dirname, filenames in walked_path:

            # iterate over all files in a folder
            for filename in filenames:

                if (not "manifest" in filename[0] and not fileNames) or (
                    fileNames and filename[0] in fileNames
                ):

                    # don't add manifest to list of files unless it is specified in the list of specified fileNames; return all found files
                    # except the manifest if no fileNames have been specified
                    # TODO: refactor for clarity/maintainability

                    if fullpath:
                        # append directory path to filename
                        filename = (dirpath[0] + "/" + filename[0], filename[1])

                    # add file name file id tuple, rearranged so that id is first and name follows
                    file_list.append(filename[::-1])

        return file_list

    def _get_manifest_id(self, manifest: pd.DataFrame) -> str:
        """If both censored and uncensored manifests are present, return uncensored manifest; if only one manifest is present, return manifest id of that manifest; if more than two manifests are present, return the manifest id of the first one. 
        Args:
        manifest: a dataframe contains name and id of manifests in a given asset view

        Return: 
        manifest_syn_id: id of a given censored or uncensored manifest
        """ 
        censored_regex=re.compile('.*censored.*')
        censored = manifest['name'].str.contains(censored_regex)
        if any(censored):
            # Try to use uncensored manifest first
            not_censored=~censored
            if any(not_censored):
                manifest_syn_id=manifest[not_censored]["id"][0]
            # if only censored manifests are available, just use the first censored manifest
            else: 
                manifest_syn_id = manifest["id"][0]

        #otherwise, use the first (implied only) version that exists
        else:
            manifest_syn_id = manifest["id"][0]
        
        return manifest_syn_id

    def getDatasetManifest(
        self, datasetId: str, downloadFile: bool = False, newManifestName: str='',
    ) -> Union[str, File]:
        """Gets the manifest associated with a given dataset.

        Args:
            datasetId: synapse ID of a storage dataset.
            downloadFile: boolean argument indicating if manifest file in dataset should be downloaded or not.
            newManifestName: new name of a manifest that gets downloaded 

        Returns:
            manifest_syn_id (String): Synapse ID of exisiting manifest file.
            manifest_data (synapseclient.entity.File): Synapse entity if downloadFile is True.
            "" (String): No pre-exisiting manifest in dataset.
        """
        manifest_data = ""

        # get a list of files containing the manifest for this dataset (if any)
        all_files = self.storageFileviewTable

        # construct regex based on manifest basename in the config 
        manifest_re=re.compile(os.path.basename(self.manifest)+".*.[tc]sv")

        # search manifest based on given manifest basename regex above
        # and return a dataframe containing name and id of manifests in a given asset view
        manifest = all_files[
            (all_files['name'].str.contains(manifest_re,regex=True))
            & (all_files["parentId"] == datasetId)
        ]

        manifest = manifest[["id", "name"]]
        
        # if there is no pre-exisiting manifest in the specified dataset
        if manifest.empty:
            logger.warning(f"Could not find a manifest that fits basename {self.manifest} in asset view and dataset {datasetId}")
            return ""

        # if there is an exisiting manifest
        else:
            manifest_syn_id = self._get_manifest_id(manifest)
            if downloadFile: 
                md = ManifestDownload(self.syn, manifest_id=manifest_syn_id)
                manifest_data = ManifestDownload.download_manifest(md, newManifestName=newManifestName, manifest_df=manifest)
                ## TO DO: revisit how downstream code handle manifest_data. If the downstream code would break when manifest_data is an empty string, 
                ## then we should catch the error here without returning an empty string. 
                if not manifest_data:
                    logger.debug(f"No manifest data returned. Please check if you have successfully downloaded manifest: {manifest_syn_id}")
                return manifest_data
            return manifest_syn_id

    def getDataTypeFromManifest(self, manifestId:str):
        """Fetch a manifest and return data types of all columns
        Args: 
            manifestId: synapse ID of a manifest
        """
        # get manifest file path 
        manifest_filepath = self.syn.get(manifestId).path

        # load manifest dataframe 
        manifest = load_df(manifest_filepath, preserve_raw_input=False, data_model=False)

        # convert the dataFrame to use best possible dtypes.
        manifest_new = manifest.convert_dtypes()

        # get data types of columns
        result = manifest_new.dtypes.to_frame('dtypes').reset_index()

        # return the result as a dictionary 
        result_dict = result.set_index('index')['dtypes'].astype(str).to_dict()


        return result_dict

    def updateDatasetManifestFiles(self, sg: SchemaGenerator, datasetId: str, store:bool = True) -> Union[Tuple[str, pd.DataFrame], None]:
        """Fetch the names and entity IDs of all current files in dataset in store, if any; update dataset's manifest with new files, if any.

        Args:
            datasetId: synapse ID of a storage dataset.
            store: if set to True store updated manifest in asset store; if set to False
            return a Pandas dataframe containing updated manifest but do not store to asset store


        Returns:
            Synapse ID of updated manifest and Pandas dataframe containing the updated manifest. 
            If there is no existing manifest return None
        """

        # get existing manifest Synapse ID
        manifest_id = self.getDatasetManifest(datasetId)

        # if there is no manifest return None
        if not manifest_id:
            return None

        manifest_filepath = self.syn.get(manifest_id).path
        manifest = load_df(manifest_filepath)

        # get current list of files
        dataset_files = self.getFilesInStorageDataset(datasetId)

        # update manifest with additional filenames, if any
        # note that if there is an existing manifest and there are files in the dataset
        # the columns Filename and entityId are assumed to be present in manifest schema
        # TODO: use idiomatic panda syntax
        if dataset_files:
            new_files = {"Filename": [], "entityId": []}

            # find new files if any
            for file_id, file_name in dataset_files:
                if not file_id in manifest["entityId"].values:
                    new_files["Filename"].append(file_name)
                    new_files["entityId"].append(file_id)

            # update manifest so that it contain new files
            new_files = pd.DataFrame(new_files)
            manifest = (
                pd.concat([manifest, new_files], sort=False)
                .reset_index()
                .drop("index", axis=1)
            )

            # update the manifest file, so that it contains the relevant entity IDs
            if store:
                manifest.to_csv(manifest_filepath, index=False)

                # store manifest and update associated metadata with manifest on Synapse
                manifest_id = self.associateMetadataWithFiles(sg, manifest_filepath, datasetId)

        manifest = manifest.fillna("") 
        
        return manifest_id, manifest

    def getProjectManifests(self, projectId: str) -> List[str]:
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
        component=None
        entity=None
        manifests = []

        datasets = self.getStorageDatasetsInProject(projectId)

        for (datasetId, datasetName) in datasets:
            # encode information about the manifest in a simple list (so that R clients can unpack it)
            # eventually can serialize differently
                
            # Get synID of manifest for a dataset
            manifestId = self.getDatasetManifest(datasetId)

            # If a manifest exists, get the annotations for it, else return base 'manifest' tuple
            if manifestId:
                annotations = self.getFileAnnotations(manifestId)

                # If manifest has annotations specifying component, use that
                if annotations and 'Component' in annotations:
                    component = annotations['Component']
                    entity = self.syn.get(manifestId, downloadFile=False)
                    manifest_name = entity["properties"]["name"]

                # otherwise download the manifest and parse for information
                elif not annotations or 'Component' not in annotations:
                    logging.debug(
                        f"No component annotations have been found for manifest {manifestId}. "
                        "The manifest will be downloaded and parsed instead. "
                        "For increased speed, add component annotations to manifest."
                        )

                    manifest_info = self.getDatasetManifest(datasetId,downloadFile=True)
                    manifest_name = manifest_info["properties"].get("name", "")

                    if not manifest_name:
                        logger.error(f'Failed to download manifests from {datasetId}') 

                    manifest_path = manifest_info["path"]

                    manifest_df = load_df(manifest_path)

                    # Get component from component column if it exists
                    if "Component" in manifest_df and not manifest_df["Component"].empty:
                        list(set(manifest_df['Component']))
                        component = list(set(manifest_df["Component"]))

                        #Added to address issues raised during DCA testing
                        if '' in component:
                            component.remove('')

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
                logging.debug(f"Manifest {manifestId} does not have an associated Component")
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

    def upload_project_manifests_to_synapse(self, projectId: str) -> List[str]:
        """Upload all metadata manifest files across all datasets in a specified project as tables in Synapse.

        Returns: String of all the manifest_table_ids of all the manifests that have been loaded.
        """

        manifests = []
        manifest_loaded = []
        datasets = self.getStorageDatasetsInProject(projectId)

        for (datasetId, datasetName) in datasets:
            # encode information about the manifest in a simple list (so that R clients can unpack it)
            # eventually can serialize differently

            manifest = ((datasetId, datasetName), ("", ""), ("", ""))

            manifest_info = self.getDatasetManifest(datasetId, downloadFile=True)
            if manifest_info:
                manifest_id = manifest_info["properties"]["id"]
                manifest_name = manifest_info["properties"]["name"]
                manifest_path = manifest_info["path"]
                manifest_df = load_df(manifest_path)
                manifest_table_id = uploadDB(manifest, datasetId, datasetName)
                manifest_loaded.append(datasetName)
        return manifest_loaded

    def upload_annotated_project_manifests_to_synapse(self, projectId:str, path_to_json_ld: str, dry_run: bool = False) -> List[str]:
        '''
        Purpose:
            For all manifests in a project, upload them as a table and add annotations manifest csv.
            Assumes the manifest is already present as a CSV in a dataset in the project.

        '''

        sg = SchemaGenerator(path_to_json_ld)
        manifests = []
        manifest_loaded = []
        datasets = self.getStorageDatasetsInProject(projectId)
        for (datasetId, datasetName) in datasets:
            # encode information about the manifest in a simple list (so that R clients can unpack it)
            # eventually can serialize differently

            manifest = ((datasetId, datasetName), ("", ""), ("", ""))
            manifests.append(manifest)

            manifest_info = self.getDatasetManifest(datasetId, downloadFile=True)

            if manifest_info:
                manifest_id = manifest_info["properties"]["id"]
                manifest_name = manifest_info["properties"]["name"]
                manifest_path = manifest_info["path"]
                manifest = ((datasetId, datasetName), (manifest_id, manifest_name), ("", ""))
                if not dry_run:
                    manifest_syn_id = self.associateMetadataWithFiles(sg, manifest_path, datasetId, manifest_record_type='table')
                manifest_loaded.append(manifest)
            
        return manifests, manifest_loaded


    def move_entities_to_new_project(self, projectId: str, newProjectId: str, returnEntities: bool = False, dry_run: bool = False):
        """
        For each manifest csv in a project, look for all the entitiy ids that are associated.
        Look up the entitiy in the files, move the entity to new project.
        """

        manifests = []
        manifest_loaded = []
        datasets = self.getStorageDatasetsInProject(projectId)
        if datasets:
            for (datasetId, datasetName) in datasets:
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

                    manifest = ((datasetId, datasetName), (manifest_id, manifest_name), ("", ""))
                    manifest_loaded.append(manifest)

                    annotation_entities = self.storageFileviewTable[
                            (self.storageFileviewTable['id'].isin(manifest_df['entityId']))
                            & (self.storageFileviewTable['type'] == 'folder')
                        ]['id']

                    if returnEntities:
                        for entityId in annotation_entities: 
                            if not dry_run:
                                self.syn.move(entityId, datasetId)
                            else:
                                logging.info(f"{entityId} will be moved to folder {datasetId}.")
                    else:                
                        # generate project folder
                        archive_project_folder = Folder(projectId+'_archive', parent = newProjectId)
                        archive_project_folder = self.syn.store(archive_project_folder)
        
                        # generate dataset folder
                        dataset_archive_folder = Folder("_".join([datasetId,datasetName,'archive']), parent = archive_project_folder.id)
                        dataset_archive_folder = self.syn.store(dataset_archive_folder)                    

                        for entityId in annotation_entities:
                            # move entities to folder
                            if not dry_run:
                                self.syn.move(entityId, dataset_archive_folder.id)
                            else:
                                logging.info(f"{entityId} will be moved to folder {dataset_archive_folder.id}.")
        else:
            raise LookupError(
                f"No datasets were found in the specified project: {projectId}. Re-check specified master_fileview in CONFIG and retry."
            )
        return manifests, manifest_loaded

    def get_synapse_table(self, synapse_id: str) -> Tuple[pd.DataFrame, CsvFileTable]:
        """Download synapse table as a pd dataframe; return table schema and etags as results too

        Args:
            synapse_id: synapse ID of the table to query
        """

        results = self.syn.tableQuery("SELECT * FROM {}".format(synapse_id))
        df = results.asDataFrame(rowIdAndVersionInIndex=False)

        return df, results

    def _get_tables(self, datasetId: str = None, projectId: str = None) -> List[Table]:
        if projectId:
            project = projectId
        elif datasetId:
            project = self.syn.get(self.getDatasetProject(datasetId))
        
        return list(self.syn.getChildren(project, includeTypes=["table"]))

    def get_table_info(self, datasetId: str = None, projectId: str = None) -> List[str]:
        """Gets the names of the tables in the schema
        Can pass in a synID for a dataset or project
        Returns:
            list[str]: A list of table names
        """
        tables = self._get_tables(datasetId = datasetId, projectId = projectId)
        if tables:
            return {table["name"]: table["id"] for table in tables}
        else: 
            return {None:None}

    @missing_entity_handler
    def uploadDB(self, 
        sg: SchemaGenerator, 
        manifest: pd.DataFrame, 
        datasetId: str, 
        table_name: str, 
        restrict: bool = False, 
        useSchemaLabel: bool = True, 
        table_manipulation: str = 'replace',
        ):
        """
        Method to upload a database to an asset store. In synapse, this will upload a metadata table
        
        Args:
            se: schemaExplorer object
            manifest: pd.Df manifest to upload
            datasetId: synID of the dataset for the manifest
            table_name: name of the table to be uploaded
            restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions 
            useSchemaLabel: bool whether to use schemaLabel (True) or display label (False)
            existingTableId: str of the synId of the existing table, if one already exists
            table_manipulation: str, 'replace' or 'upsert', in the case where a manifest already exists, should the new metadata replace the existing (replace) or be added to it (upsert)

        Returns:
            manifest_table_id: synID of the uploaded table
            manifest: the original manifset
            table_manifest: manifest formatted appropriately for the table
        
        """
        

        col_schema, table_manifest = self.formatDB(sg, manifest, useSchemaLabel)

        manifest_table_id = self.buildDB(datasetId, table_name, col_schema, table_manifest, table_manipulation, sg, restrict,)

        return manifest_table_id, manifest, table_manifest

    def formatDB(self, sg, manifest, useSchemaLabel):
        """
        Method to format a manifest appropriatly for upload as table
        
        Args:
            se: schemaExplorer object
            manifest: pd.Df manifest to upload
            useSchemaLabel: bool whether to use schemaLabel (True) or display label (False)

        Returns:
            col_schema: schema for table columns: type, size, etc
            table_manifest: formatted manifest
        
        """
        # Rename the manifest columns to display names to match fileview

        blacklist_chars = ['(', ')', '.', ' ', '-']
        manifest_columns = manifest.columns.tolist()

        table_manifest=deepcopy(manifest)

        if useSchemaLabel:
            cols = [
                sg.se.get_class_label_from_display_name(
                    str(col)
                    ).translate({ord(x): '' for x in blacklist_chars})
                for col in manifest_columns
            ]

            cols = list(map(lambda x: x.replace('EntityId', 'entityId'), cols))


            # Reset column names in table manifest
            table_manifest.columns = cols

        #move entity id to end of df
        entity_col = table_manifest.pop('entityId')
        table_manifest.insert(len(table_manifest.columns), 'entityId', entity_col)

        # Get the column schema
        col_schema = as_table_columns(table_manifest)

        # Set Id column length to 64 (for some reason not being auto set.)
        for i, col in enumerate(col_schema):
            if col['name'].lower() == 'id':
                col_schema[i]['maximumSize'] = 64

        return col_schema, table_manifest

    def buildDB(self,  
        datasetId: str, 
        table_name: str, 
        col_schema: List,
        table_manifest: pd.DataFrame,
        table_manipulation: str,
        sg: SchemaGenerator,  
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
        table_info = self.get_table_info(datasetId = datasetId)
        # Put table manifest onto synapse
        schema = Schema(name=table_name, columns=col_schema, parent=self.getDatasetProject(datasetId))

        
        if not table_manipulation or table_name not in table_info.keys():
            manifest_table_id = TableOperations.createTable(self, tableToLoad=table_manifest, tableName=table_name, datasetId=datasetId, columnTypeDict=col_schema, specifySchema=True, restrict=restrict)
        elif table_name in table_info.keys() and table_info[table_name]:

            if table_manipulation.lower() == 'replace':
                manifest_table_id = TableOperations.replaceTable(self, tableToLoad=table_manifest, tableName=table_name, existingTableId=table_info[table_name], specifySchema = True, datasetId = datasetId, columnTypeDict=col_schema, restrict=restrict)
            elif table_manipulation.lower() == 'upsert':
                manifest_table_id = TableOperations.upsertTable(self, sg=sg, tableToLoad = table_manifest, tableName=table_name, existingTableId=table_info[table_name], datasetId=datasetId)
            elif table_manipulation.lower() == 'update':
                manifest_table_id = TableOperations.updateTable(self, tableToLoad=table_manifest, existingTableId=table_info[table_name], restrict=restrict)



        if table_manipulation and table_manipulation.lower() == 'upsert':
            existing_tables=self.get_table_info(datasetId=datasetId)
            tableId=existing_tables[table_name]
            annos = self.syn.get_annotations(tableId)
            annos['primary_key'] = table_manifest['Component'][0] + "_id"
            annos = self.syn.set_annotations(annos)

        return manifest_table_id


    def upload_manifest_file(self, manifest, metadataManifestPath, datasetId, restrict_manifest, component_name = ''):
        # Update manifest to have the new entityId column
        manifest.to_csv(metadataManifestPath, index=False)

        # store manifest to Synapse as a CSV
        # update file name
        file_name_full = metadataManifestPath.split('/')[-1]
        file_extension = file_name_full.split('.')[-1]

        # Differentiate "censored" and "uncensored" manifest
        if "censored" in file_name_full: 
            file_name_new = os.path.basename(CONFIG["synapse"]["manifest_basename"]) + "_" + component_name + "_censored" + '.' + file_extension
        else: 
            file_name_new = os.path.basename(CONFIG["synapse"]["manifest_basename"]) + "_" + component_name + '.' + file_extension

        manifestSynapseFile = File(
            metadataManifestPath,
            description="Manifest for dataset " + datasetId,
            parent=datasetId,
            name=file_name_new
        )

        manifest_synapse_file_id = self.syn.store(manifestSynapseFile, isRestricted = restrict_manifest).id
        changeFileMetaData(syn = self.syn, entity = manifest_synapse_file_id, downloadAs = file_name_new)
        
        return manifest_synapse_file_id

    @missing_entity_handler
    def format_row_annotations(self, se, sg, row, entityId, useSchemaLabel, hideBlanks):
        # prepare metadata for Synapse storage (resolve display name into a name that Synapse annotations support (e.g no spaces, parenthesis)
        # note: the removal of special characters, will apply only to annotation keys; we are not altering the manifest
        # this could create a divergence between manifest column and annotations. this should be ok for most use cases.
        # columns with special characters are outside of the schema
        metadataSyn = {}
        blacklist_chars = ['(', ')', '.', ' ', '-']
        
        for k, v in row.to_dict().items():

            if useSchemaLabel:
                keySyn = se.get_class_label_from_display_name(str(k)).translate({ord(x): '' for x in blacklist_chars})
            else:
                keySyn = str(k)

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
        # set annotation(s) for the various objects/items in a dataset on Synapse
        annos = self.syn.get_annotations(entityId)
        csv_list_regex=comma_separated_list_regex()
        for anno_k, anno_v in metadataSyn.items():
            
            # Remove keys with nan or empty string values from dict of annotations to be uploaded
            # if present on current data annotation
            if hideBlanks and (anno_v == '' or (isinstance(anno_v,float) and np.isnan(anno_v))):
                annos.pop(anno_k) if anno_k in annos.keys() else annos
            # Otherwise save annotation as approrpriate
            else:
                if isinstance(anno_v,float) and np.isnan(anno_v):
                        annos[anno_k] = ""
                elif isinstance(anno_v,str) and re.fullmatch(csv_list_regex, anno_v) and rule_in_rule_list('list', sg.get_node_validation_rules(anno_k)):
                    annos[anno_k] = anno_v.split(",")
                else:
                    annos[anno_k] = anno_v
                
        return annos

    @missing_entity_handler
    def format_manifest_annotations(self, manifest, manifest_synapse_id):
        '''
        Set annotations for the manifest (as a whole) so they can be applied to the manifest table or csv.
        For now just getting the Component.
        '''
        
        entity = self.syn.get(manifest_synapse_id, downloadFile=False)
        is_file = entity.concreteType.endswith(".FileEntity")
        is_table = entity.concreteType.endswith(".TableEntity")

        if is_file:

            # Get file metadata
            metadata = self.getFileAnnotations(manifest_synapse_id)

            # If there is a defined component add it to the metadata.
            if 'Component' in manifest.columns:
                # Gather component information
                component = manifest['Component'].unique()
                
                # Double check that only a single component is listed, else raise an error.
                try:
                    len(component) == 1
                except ValueError as err:
                    raise ValueError(
                        f"Manifest has more than one component. Please check manifest and resubmit."
                    ) from err

                # Add component to metadata
                metadata['Component'] = component[0]
        
        elif is_table:
            # Get table metadata
            metadata = self.getTableAnnotations(manifest_synapse_id)
        
        # Get annotations
        annos = self.syn.get_annotations(manifest_synapse_id)

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

        # get a schema explorer object to ensure schema attribute names used in manifest are translated to schema labels for synapse annotations
        se = SchemaExplorer()

        # Create table name here.
        if 'Component' in manifest.columns:
            table_name = manifest['Component'][0].lower() + '_synapse_storage_manifest_table'
        else:
            table_name = 'synapse_storage_manifest_table'

        # Upload manifest as a table and get the SynID and manifest
        manifest_synapse_table_id, manifest, table_manifest = self.upload_format_manifest_table(
                                                    se, manifest, datasetId, table_name, restrict = restrict_manifest, useSchemaLabel=useSchemaLabel,)
            
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

    def _read_manifest(self, metadataManifestPath:str) -> pd.DataFrame:
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
            load_args={
                "dtype":"string",
            }
            manifest = load_df(metadataManifestPath, preserve_raw_input = False, **load_args)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"No manifest file was found at this path: {metadataManifestPath}"
            ) from err
        return manifest

    def _add_id_columns_to_manifest(self, manifest: pd.DataFrame, sg: SchemaGenerator):
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
                uuid_col_in_schema = sg.se.is_class_in_schema('Uuid') or sg.se.is_class_in_schema('uuid')      
            except (KeyError):
                uuid_col_in_schema = False

            # Rename `Uuid` column if it wasn't specified in the schema
            if col_in_dataframe("Uuid", manifest) and not uuid_col_in_schema:
                manifest.rename(columns={'Uuid': 'Id'}, inplace=True)
            # If no `Uuid` column exists or it is specified in the schema, create a new `Id` column
            else:
                manifest["Id"] = ''

        for idx,row in manifest.iterrows():
            if not row["Id"]:
                gen_uuid = str(uuid.uuid4())
                row["Id"] = gen_uuid
                manifest.loc[idx, 'Id'] = gen_uuid

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
        if 'Component' in manifest.columns:
            component_name = manifest['Component'][0].lower()
            table_name = component_name + '_synapse_storage_manifest_table'
        else:
            component_name = ''
            table_name = 'synapse_storage_manifest_table'
        return table_name, component_name

    def _add_annotations(self, se, schemaGenerator, row, entityId, useSchemaLabel, hideBlanks):
        """Helper function to format and add annotations to entities in Synapse.
        Args:
            se: schemaExplorer object,
            schemaGenerator: schemaGenerator Object.
            row: current row of manifest being processed
            entityId (str): synapseId of entity to add annotations to
            useSchemaLabel (bool): Flag to use schema label instead of display name
            hideBlanks: Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
        Returns:
            Annotations are added to entities in Synapse, no return.
        """
        # Format annotations for Synapse
        annos = self.format_row_annotations(se, schemaGenerator, row, entityId, useSchemaLabel, hideBlanks)

        if annos:
        # Store annotations for an entity folder
            self.syn.set_annotations(annos)
        return

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
        row["entityId"] = entityId
        manifest.loc[idx, "entityId"] = entityId
        return manifest, entityId

    def add_entities(
                    self,
                    se,
                    schemaGenerator,
                    manifest,
                    manifest_record_type,
                    datasetId,
                    useSchemaLabel,
                    hideBlanks,
                    manifest_synapse_table_id=''
                    ):
        '''Depending on upload type add Ids to entityId row. Add anotations to connected files.
        Args:
            se: Schema Explorer Object
            schemaGenerator: SchemaGenerator object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            datasetId (str): synapse ID of folder containing the dataset
            useSchemaLabel (bool): Default is True - use the schema label. If False, uses the display label from the schema. Attribute display names in the schema must not only include characters that are not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'.
            hideBlanks (bool): Default is false -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            manifest_synapse_table_id (str): Default is an empty string ''.
        Returns:
            manifest (pd.DataFrame): modified to add entitiyId as appropriate.

        '''
        for idx, row in manifest.iterrows():
            if not row["entityId"] and (manifest_record_type == 'file_and_entities' or 
                manifest_record_type == 'table_file_and_entities'):
                manifest, entityId = self._create_entity_id(idx, row, manifest, datasetId)
            elif not row["entityId"] and manifest_record_type == 'table_and_file':
                # If not using entityIds, fill with manifest_table_id so 
                row["entityId"] = manifest_synapse_table_id
                manifest.loc[idx, "entityId"] = manifest_synapse_table_id
                entityId = ''
            else:
                # get the entity id corresponding to this row
                entityId = row["entityId"]

            # Adding annotations to connected files.
            if entityId:
                self._add_annotations(se, schemaGenerator, row, entityId, useSchemaLabel, hideBlanks)
        return manifest

    def upload_manifest_as_table(
                            self,
                            se,
                            schemaGenerator,
                            manifest,
                            metadataManifestPath,
                            datasetId,
                            table_name,
                            component_name,
                            restrict,
                            manifest_record_type,
                            useSchemaLabel,
                            hideBlanks,
                            table_manipulation,
                            ):
        """Upload manifest to Synapse as a table and csv.
        Args:
            se: SchemaExplorer object
            schemaGenerator: SchemaGenerator Object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            metadataManifestPath: path to csv containing a validated metadata manifest.
            datasetId (str): synapse ID of folder containing the dataset
            table_name (str): Generated to name the table being uploaded.
            component_name (str): Name of the component manifest that is currently being uploaded.
            restrict (bool): Flag for censored data.
            manifest_record_type (str): valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            useSchemaLabel(bool): Default is True - use the schema label. If False, uses the display label from the schema. Attribute display names in the schema must not only include characters that are not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'.
            hideBlanks (bool): Default is False -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            table_malnipulation (str): Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
        Return:
            manifest_synapse_file_id: SynID of manifest csv uploaded to synapse.
        """      
        # Upload manifest as a table, get the ID and updated manifest.
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
                                                    schemaGenerator,
                                                    manifest,
                                                    datasetId,
                                                    table_name,
                                                    restrict,
                                                    useSchemaLabel,
                                                    table_manipulation)

        manifest = self.add_entities(se, schemaGenerator, manifest, manifest_record_type, datasetId, useSchemaLabel, hideBlanks, manifest_synapse_table_id)
        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(manifest, metadataManifestPath, datasetId, restrict, component_name = component_name)
        
        # Set annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_file_id)
        self.syn.set_annotations(manifest_annotations)
        logger.info("Associated manifest file with dataset on Synapse.")
        
        # Update manifest Synapse table with new entity id column.
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
                                                    schemaGenerator,
                                                    manifest, 
                                                    datasetId, 
                                                    table_name,  
                                                    restrict,
                                                    useSchemaLabel=useSchemaLabel,
                                                    table_manipulation='update',)

        # Set annotations for the table manifest
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_table_id)
        self.syn.set_annotations(manifest_annotations)
        return manifest_synapse_file_id

    def upload_manifest_as_csv(
                            self,
                            se,
                            schemaGenerator,
                            manifest,
                            metadataManifestPath,
                            datasetId,
                            restrict,
                            manifest_record_type,
                            useSchemaLabel,
                            hideBlanks,
                            component_name,
                            with_entities = False,):
        """Upload manifest to Synapse as a csv only.
        Args:
            se: SchemaExplorer object
            schemaGenerator: SchemaGenerator Object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            metadataManifestPath: path to csv containing a validated metadata manifest.
            datasetId (str): synapse ID of folder containing the dataset
            restrict (bool): Flag for censored data.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            useSchemaLabel (bool): Default is True - use the schema label. If False, uses the display label from the schema. Attribute display names in the schema must not only include characters that are not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'.
            hideBlanks (bool): Default is False -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            table_malnipulation (str): Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
            with_entities (bool): Default is False - Flag to indicate whether to create entityIds and add annotations.
        Return:
            manifest_synapse_file_id (str): SynID of manifest csv uploaded to synapse.
        """
        if with_entities:
            manifest = self.add_entities(se, schemaGenerator, manifest, manifest_record_type, datasetId, useSchemaLabel, hideBlanks)

        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(manifest,
                metadataManifestPath, datasetId, restrict, component_name = component_name)
        
        # Set annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_file_id)
        self.syn.set_annotations(manifest_annotations)

        logger.info("Associated manifest file with dataset on Synapse.")
        
        return manifest_synapse_file_id

    def upload_manifest_combo(
                            self,
                            se,
                            schemaGenerator,
                            manifest,
                            metadataManifestPath,
                            datasetId,
                            table_name,
                            component_name,
                            restrict,
                            manifest_record_type,
                            useSchemaLabel,
                            hideBlanks,
                            table_manipulation,
                            ):
        """Upload manifest to Synapse as a table and CSV with entities.
        Args:
            se: SchemaExplorer object
            schemaGenerator: SchemaGenerator Object
            manifest (pd.DataFrame): loaded df containing user supplied data.
            metadataManifestPath: path to csv containing a validated metadata manifest.
            datasetId (str): synapse ID of folder containing the dataset
            table_name (str): Generated to name the table being uploaded.
            component_name (str): Name of the component manifest that is currently being uploaded.
            restrict (bool): Flag for censored data.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            useSchemaLabel (bool): Default is True - use the schema label. If False, uses the display label from the schema. Attribute display names in the schema must not only include characters that are not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'.
            hideBlanks (bool): Default is False -Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            table_malnipulation (str): Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
        Return:
            manifest_synapse_file_id (str): SynID of manifest csv uploaded to synapse.
        """
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
                                                    se,
                                                    manifest,
                                                    datasetId,
                                                    table_name,
                                                    restrict,
                                                    useSchemaLabel=useSchemaLabel,
                                                    table_manipulation=table_manipulation,)

        manifest = self.add_entities(se, schemaGenerator, manifest, manifest_record_type, datasetId, useSchemaLabel, hideBlanks, manifest_synapse_table_id)
        
        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.upload_manifest_file(manifest, metadataManifestPath, datasetId, restrict, component_name)
        
        # Set annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_file_id)
        self.syn.set_annotations(manifest_annotations)
        logger.info("Associated manifest file with dataset on Synapse.")
        
        # Update manifest Synapse table with new entity id column.
        manifest_synapse_table_id, manifest, table_manifest = self.uploadDB(
                                                                se,
                                                                manifest,
                                                                datasetId,
                                                                table_name,
                                                                restrict,
                                                                useSchemaLabel=useSchemaLabel,
                                                                table_manipulation='update',)

        # Set annotations for the table manifest
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_table_id)
        self.syn.set_annotations(manifest_annotations)
        return manifest_synapse_file_id

    def associateMetadataWithFiles(
        self, schemaGenerator: SchemaGenerator, metadataManifestPath: str, datasetId: str, manifest_record_type: str = 'table_file_and_entities', 
        useSchemaLabel: bool = True, hideBlanks: bool = False, restrict_manifest = False, table_manipulation: str = 'replace',
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
            schemaGenerator: SchemaGenerator Object
            metadataManifestPath: path to csv containing a validated metadata manifest.
            The manifest should include a column entityId containing synapse IDs of files/entities to be associated with metadata, if that is applicable to the dataset type.
            Some datasets, e.g. clinical data, do not contain file id's, but data is stored in a table: one row per item.
            In this case, the system creates a file on Synapse for each row in the table (e.g. patient, biospecimen) and associates the columnset data as metadata/annotations to his file.
            datasetId: synapse ID of folder containing the dataset
            manifest_record_type: Default value is 'table_file_and_entities'. valid values are 'file_only', 'file_and_entities', 'table_and_file' or 'table_file_and_entities'. 'file_and_entities' will store the manifest as a csv and create Synapse files for each row in the manifest.'table_and_file' will store the manifest as a table and a csv on Synapse. 'file_only' will store the manifest as a csv only on Synapse. 'table_file_and_entities' will perform the options file_with_entites and table in combination.
            useSchemaLabel: Default is True - use the schema label. If False, uses the display label from the schema. Attribute display names in the schema must not only include characters that are not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'.
            hideBlanks: Default is false. Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
            restrict_manifest (bool): Default is false. Flag for censored data.
            table_malnipulation (str): Default is 'replace'. Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'.
        Returns:
            manifest_synapse_file_id: SynID of manifest csv uploaded to synapse.
        """
        # Read new manifest CSV:
        manifest = self._read_manifest(metadataManifestPath)
        manifest = self._add_id_columns_to_manifest(manifest, schemaGenerator)

        # get a schema explorer object to ensure schema attribute names used in manifest are translated to schema labels for synapse annotations
        se = SchemaExplorer()

        table_name, component_name = self._generate_table_name(manifest)

        # Upload manifest to synapse based on user input (manifest_record_type)
        
        if manifest_record_type == "file_only":
            manifest_synapse_file_id = self.upload_manifest_as_csv(
                                        se,
                                        schemaGenerator,
                                        manifest,
                                        metadataManifestPath,
                                        datasetId=datasetId,
                                        restrict=restrict_manifest,
                                        useSchemaLabel=useSchemaLabel,
                                        hideBlanks=hideBlanks,
                                        manifest_record_type=manifest_record_type,
                                        component_name = component_name,
                                        with_entities = False,
                                        )
        elif manifest_record_type == "table_and_file":
            manifest_synapse_file_id = self.upload_manifest_as_table(
                                        se,
                                        schemaGenerator,
                                        manifest,
                                        metadataManifestPath,
                                        datasetId=datasetId,
                                        table_name=table_name,
                                        component_name=component_name,
                                        restrict=restrict_manifest,
                                        useSchemaLabel=useSchemaLabel,
                                        hideBlanks=hideBlanks,
                                        manifest_record_type=manifest_record_type,
                                        table_manipulation=table_manipulation,
                                        )
        elif manifest_record_type == "file_and_entities":
            manifest_synapse_file_id = self.upload_manifest_as_csv( 
                                        se,
                                        schemaGenerator,
                                        manifest,
                                        metadataManifestPath,
                                        datasetId=datasetId,
                                        restrict=restrict_manifest,
                                        useSchemaLabel=useSchemaLabel,
                                        hideBlanks=hideBlanks,
                                        manifest_record_type=manifest_record_type,
                                        component_name = component_name,
                                        with_entities=True,
                                        )
        elif manifest_record_type == "table_file_and_entities":
            manifest_synapse_file_id = self.upload_manifest_combo(
                                        se,
                                        schemaGenerator,
                                        manifest,
                                        metadataManifestPath,
                                        datasetId=datasetId,
                                        table_name=table_name,
                                        component_name=component_name,
                                        restrict=restrict_manifest,
                                        useSchemaLabel=useSchemaLabel,
                                        hideBlanks=hideBlanks,
                                        manifest_record_type=manifest_record_type,
                                        table_manipulation=table_manipulation,
                                        )
        else:
            raise ValueError("Please enter a valid manifest_record_type.")
        return manifest_synapse_file_id

    def getTableAnnotations(self, table_id:str):
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
            entity = self.syn.get(table_id, downloadFile=False)
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
            entity = self.syn.get(fileId, downloadFile=False)
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
            except (SynapseAuthenticationError, SynapseHTTPError):
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

        if 'Filename' not in table.columns:
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

    @retry(stop = stop_after_attempt(5), 
            wait = wait_chain(*[wait_fixed(10) for i in range (2)] + 
                    [wait_fixed(15) for i in range(2)] + 
                    [wait_fixed(20)]),
            retry=retry_if_exception_type(LookupError),
            retry_error_callback = raise_final_error)

    def checkIfinAssetView(self, syn_id) -> str:
        # get data in administrative fileview for this pipeline
        assetViewTable = self.getStorageFileviewTable()
        all_files = list(assetViewTable["id"])
        if syn_id in all_files: 
            return True
        else: 
            return False

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
            self._query_fileview()
            # Subset main file view
            dataset_index = self.storageFileviewTable["id"] == datasetId
            dataset_row = self.storageFileviewTable[dataset_index]


        # Return `projectId` for given row if only one found
        if len(dataset_row) == 1:
            dataset_project = dataset_row["projectId"].values[0]
            return dataset_project

        # Otherwise, check if already project itself
        try:
            syn_object = self.syn.get(datasetId)
            if syn_object.properties["concreteType"].endswith("Project"):
                return datasetId
        except SynapseHTTPError:
            raise PermissionError(
                f"The given dataset ({datasetId}) isn't accessible with this "
                "user. This might be caused by a typo in the dataset Synapse ID."
            )

        # If not, then assume dataset not in file view
        raise LookupError (
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

            #TODO clean up dictionary for compactness (e.g. remove redundant 'name' key)
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


    def createTable(synStore: SynapseStorage, tableToLoad: pd.DataFrame = None, tableName: str = None, datasetId: str = None, columnTypeDict: dict = None, specifySchema: bool = True, restrict: bool = False):
        """
        Method to create a table from a metadata manifest and upload it to synapse
        
        Args:
            tableToLoad: manifest formatted appropriately for the table
            tableName: name of the table to be uploaded
            datasetId: synID of the dataset for the manifest
            columnTypeDict: dictionary schema for table columns: type, size, etc
            specifySchema: to specify a specific schema for the table format          
            restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions 
            

        Returns:
            table.schema.id: synID of the newly created table
        """

        datasetEntity = synStore.syn.get(datasetId, downloadFile = False)
        datasetName = datasetEntity.name
        table_schema_by_cname = synStore._get_table_schema_by_cname(columnTypeDict) 

        if not tableName:
            tableName = datasetName + 'table'
        datasetParentProject = synStore.getDatasetProject(datasetId)
        if specifySchema:
            if columnTypeDict == {}:
                logger.error("Did not provide a columnTypeDict.")
            #create list of columns:
            cols = []
            for col in tableToLoad.columns:
                if col in table_schema_by_cname:
                    col_type = table_schema_by_cname[col]['columnType']
                    max_size = table_schema_by_cname[col]['maximumSize'] if 'maximumSize' in table_schema_by_cname[col].keys() else 100
                    max_list_len = 250
                    if max_size and max_list_len:
                        cols.append(Column(name=col, columnType=col_type, 
                            maximumSize=max_size, maximumListLength=max_list_len))
                    elif max_size:
                        cols.append(Column(name=col, columnType=col_type, 
                            maximumSize=max_size))
                    else:
                        cols.append(Column(name=col, columnType=col_type))
                else:
                    #TODO add warning that the given col was not found and it's max size is set to 100
                    cols.append(Column(name=col, columnType='STRING', maximumSize=100))
            schema = Schema(name=tableName, columns=cols, parent=datasetParentProject)
            table = Table(schema, tableToLoad)
            table = synStore.syn.store(table, isRestricted = restrict)
            return table.schema.id
        else:
            # For just uploading the tables to synapse using default
            # column types.
            table = build_table(tableName, datasetParentProject, tableToLoad)
            table = synStore.syn.store(table, isRestricted = restrict)
            return table.schema.id

    def replaceTable(synStore: SynapseStorage, tableToLoad: pd.DataFrame = None, tableName: str = None, existingTableId: str = None, specifySchema: bool = True, datasetId: str = None, columnTypeDict: dict = None, restrict: bool = False):
        """
        Method to replace an existing table on synapse with metadata from a new manifest
        
        Args:
            tableToLoad: manifest formatted appropriately for the table
            tableName: name of the table to be uploaded
            existingTableId: synId of the existing table to be replaced
            specifySchema: to infer a schema for the table format      
            datasetId: synID of the dataset for the manifest    
            columnTypeDict: dictionary schema for table columns: type, size, etc
            restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions 
            

        Returns:
           existingTableId: synID of the already existing table that had its metadata replaced
        """
        datasetEntity = synStore.syn.get(datasetId, downloadFile = False)
        datasetName = datasetEntity.name
        table_schema_by_cname = synStore._get_table_schema_by_cname(columnTypeDict) 
        existing_table, existing_results = synStore.get_synapse_table(existingTableId)
        # remove rows
        synStore.syn.delete(existing_results)
        # wait for row deletion to finish on synapse before getting empty table
        sleep(10)
        
        # removes all current columns
        current_table = synStore.syn.get(existingTableId)
        current_columns = synStore.syn.getTableColumns(current_table)
        for col in current_columns:
            current_table.removeColumn(col)

        if not tableName:
            tableName = datasetName + 'table'
        
        # Process columns according to manifest entries
        table_schema_by_cname = synStore._get_table_schema_by_cname(columnTypeDict) 
        datasetParentProject = synStore.getDatasetProject(datasetId)
        if specifySchema:
            if columnTypeDict == {}:
                logger.error("Did not provide a columnTypeDict.")
            #create list of columns:
            cols = []
            
            for col in tableToLoad.columns:
                
                if col in table_schema_by_cname:
                    col_type = table_schema_by_cname[col]['columnType']
                    max_size = table_schema_by_cname[col]['maximumSize'] if 'maximumSize' in table_schema_by_cname[col].keys() else 100
                    max_list_len = 250
                    if max_size and max_list_len:
                        cols.append(Column(name=col, columnType=col_type, 
                            maximumSize=max_size, maximumListLength=max_list_len))
                    elif max_size:
                        cols.append(Column(name=col, columnType=col_type, 
                            maximumSize=max_size))
                    else:
                        cols.append(Column(name=col, columnType=col_type))
                else:
                    
                    #TODO add warning that the given col was not found and it's max size is set to 100
                    cols.append(Column(name=col, columnType='STRING', maximumSize=100))
            
            # adds new columns to schema
            for col in cols:
                current_table.addColumn(col)
            synStore.syn.store(current_table, isRestricted = restrict)

            # wait for synapse store to finish
            sleep(1)

            # build schema and table from columns and store with necessary restrictions
            schema = Schema(name=tableName, columns=cols, parent=datasetParentProject)
            schema.id = existingTableId
            table = Table(schema, tableToLoad, etag = existing_results.etag)
            table = synStore.syn.store(table, isRestricted = restrict)
        else:
            logging.error("Must specify a schema for table replacements")

        # remove system metadata from manifest
        existing_table.drop(columns = ['ROW_ID', 'ROW_VERSION'], inplace = True)
        return existingTableId
    

    def _get_schematic_db_creds(synStore: SynapseStorage):
        username = None
        authtoken = None


        # Get access token from environment variable if available
        # Primarily useful for testing environments, with other possible usefulness for containers
        env_access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")
        if env_access_token:
            authtoken = env_access_token
            return username, authtoken

        # Get token from authorization header
        # Primarily useful for API endpoint functionality
        if 'Authorization' in synStore.syn.default_headers:
            authtoken = synStore.syn.default_headers['Authorization'].split('Bearer ')[-1]
            return username, authtoken

        # retrive credentials from synapse object
        # Primarily useful for local users, could only be stored here when a .synapseConfig file is used, but including to be safe
        synapse_object_creds = synStore.syn.credentials
        if hasattr(synapse_object_creds, 'username'):
            username = synapse_object_creds.username
        if hasattr(synapse_object_creds, '_token'):
            authtoken = synapse_object_creds.secret

        # Try getting creds from .synapseConfig file if it exists
        # Primarily useful for local users. Seems to correlate with credentials stored in synaspe object when logged in
        if os.path.exists(CONFIG.SYNAPSE_CONFIG_PATH):
            config = synStore.syn.getConfigFile(CONFIG.SYNAPSE_CONFIG_PATH)

            # check which credentials are provided in file
            if config.has_option('authentication', 'username'):
                username = config.get('authentication', 'username')
            if config.has_option('authentication', 'authtoken'):
                authtoken = config.get('authentication', 'authtoken')
        
        # raise error if required credentials are not found
        # providing an authtoken without a username did not prohibit upsert functionality, 
        # but including username gathering for completeness for schematic_db
        if not username and not authtoken:
            raise NameError(
                "Username and authtoken credentials could not be found in the environment, synapse object, or the .synapseConfig file"
            )
        if not authtoken:
            raise NameError(
                "authtoken credentials could not be found in the environment, synapse object, or the .synapseConfig file"
            )
        
        return username, authtoken

    def upsertTable(synStore: SynapseStorage, sg: SchemaGenerator, tableToLoad: pd.DataFrame = None, tableName: str = None, existingTableId: str = None,  datasetId: str = None):
        """
        Method to upsert rows from a new manifest into an existing table on synapse
        For upsert functionality to work, primary keys must follow the naming convention of <componenet>_id        
        `-tm upsert` should be used for initial table uploads if users intend to upsert into them at a later time; using 'upsert' at creation will generate the metadata necessary for upsert functionality.
        Currently it is required to use -dl/--use_display_label with table upserts.
        

        Args:
            tableToLoad: manifest formatted appropriately for the table
            tableName: name of the table to be uploaded
            existingTableId: synId of the existing table to be replaced     
            datasetId: synID of the dataset for the manifest    
            columnTypeDict: dictionary schema for table columns: type, size, etc
            

        Returns:
           existingTableId: synID of the already existing table that had its metadata replaced
        """            

        username, authtoken = TableOperations._get_schematic_db_creds(synStore)

        synConfig = SynapseConfig(username, authtoken, synStore.getDatasetProject(datasetId))
        synapseDB = SynapseDatabase(synConfig)

        try:
            # Try performing upsert
            synapseDB.upsert_table_rows(table_name=tableName, data=tableToLoad)
        except(SynapseHTTPError) as ex:
            # If error is raised because Table has old `Uuid` column and not new `Id` column, then handle and re-attempt upload
            if 'Id is not a valid column name or id' in str(ex):
                TableOperations._update_table_uuid_column(synStore, existingTableId, sg)
                synapseDB.upsert_table_rows(table_name=tableName, data=tableToLoad)
            # Raise if other error
            else:
                raise ex

        return existingTableId

    def _update_table_uuid_column(synStore: SynapseStorage, table_id: str, sg: SchemaGenerator,) -> None:
        """Removes the `Uuid` column when present, and relpaces with an `Id` column
        Used to enable backwards compatability for manifests using the old `Uuid` convention

        Args:
            table_id (str): The Synapse id of the table to be upserted into, that needs columns updated

        Returns:
            None
        """

        # Get the columns of the schema
        schema = synStore.syn.get(table_id)
        cols = synStore.syn.getTableColumns(schema)
        
        # Iterate through columns until `Uuid` column is found
        for col in cols:
            if col.name.lower() == 'uuid':
                # See if schema has `Uuid` column specified
                try:
                    uuid_col_in_schema = sg.se.is_class_in_schema(col.name)      
                except (KeyError):
                    uuid_col_in_schema = False

                # If there is, then create a new `Id` column from scratch
                if uuid_col_in_schema:
                    new_col = Column(columnType = "STRING", maximumSize = 64, name = "Id")
                    schema.addColumn(new_col)
                    schema = synStore.syn.store(schema)
                # If there is not, then use the old `Uuid` column as a basis for the new `Id` column
                else:
                    # Create a new `Id` column based off of the old `Uuid` column, and store (column is empty)
                    new_col = deepcopy(col)
                    new_col['name'] = 'Id'
                    schema.addColumn(new_col)
                    schema = synStore.syn.store(schema)
                
                
                    # Recently stored column is empty, so populated with uuid values
                    TableOperations._populate_new_id_column(synStore, table_id, schema)

                    # get the up-to-date table, remove old `Uuid` column, and store
                    sleep(1)
                    schema = synStore.syn.get(table_id)
                    schema.removeColumn(col)
                    schema = synStore.syn.store(schema)

                    # Exit iteration; only concerned with `Uuid` column
                break

        return

    def _populate_new_id_column(synStore: SynapseStorage, table_id: str, schema: Schema) -> None:
        """Copies the uuid values that were present in the column named `Uuid` to the new column named `Id`

        Args:
            table_id (str): The Synapse id of the table to be upserted into, that needs columns updated
            schema (synapseclient.table.Schema): Schema of the table columns

        Returns:
            None
        """
        # Query the table for the old `Uuid` column and new `Id` column
        results = synStore.syn.tableQuery(f"select Uuid,Id from {table_id}")
        results_df = results.asDataFrame()

        # Copy uuid values to new column, and store in table
        results_df = populate_df_col_with_another_col(results_df, 'Uuid', 'Id')
        table = synStore.syn.store(Table(schema, results_df, etag=results.etag))
        return

    def updateTable(synStore: SynapseStorage, tableToLoad: pd.DataFrame = None, existingTableId: str = None,  update_col: str = 'Id',  restrict: bool = False):
        """
        Method to update an existing table with a new column
        
        Args:
            tableToLoad: manifest formatted appropriately for the table, that contains the new column
            existingTableId: synId of the existing table to be replaced
            updateCol: column to index the old and new tables on
            restrict: bool, whether or not the manifest contains sensitive data that will need additional access restrictions 
            

        Returns:
           existingTableId: synID of the already existing table that had its metadata replaced
        """
        existing_table, existing_results = synStore.get_synapse_table(existingTableId)
        
        tableToLoad = update_df(existing_table, tableToLoad, update_col)
        # store table with existing etag data and impose restrictions as appropriate
        synStore.syn.store(Table(existingTableId, tableToLoad, etag = existing_results.etag), isRestricted = restrict)

        return existingTableId


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
            self.table = self.results.asDataFrame(rowIdAndVersionInIndex=False)
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
            to_int_fn = lambda x: "" if np.isnan(x) else str(int(x))
            self.table[col] = self.table[col].apply(to_int_fn)
        return self.table
