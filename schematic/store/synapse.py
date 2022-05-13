import os
import uuid  # used to generate unique names for entities
import json
import atexit
import logging
import secrets

# allows specifying explicit variable types
from typing import Dict, List, Tuple, Sequence, Union
from collections import OrderedDict

import numpy as np
import pandas as pd
import re
import synapseclient
import synapseutils

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
from synapseclient.table import CsvFileTable
from synapseclient.table import build_table
from synapseclient.annotations import from_synapse_annotations
from synapseclient.core.exceptions import SynapseHTTPError, SynapseAuthenticationError, SynapseUnmetAccessRestrictions
import synapseutils

import uuid

from schematic.utils.df_utils import update_df, load_df
from schematic.schemas.explorer import SchemaExplorer
from schematic.store.base import BaseStorage
from schematic.exceptions import MissingConfigValueError, AccessCredentialsError

from schematic import CONFIG

logger = logging.getLogger(__name__)


class SynapseStorage(BaseStorage):
    """Implementation of Storage interface for datasets/files stored on Synapse.
    Provides utilities to list files in a specific project; update files annotations, create fileviews, etc.

    TODO: Need to define the interface and rename and/or refactor some of the methods below.
    """

    def __init__(
        self,
        token: str = None,  # optional parameter retrieved from browser cookie
        access_token: str = None,
        input_token: str = None,
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

        self.syn = self.login(token, access_token, input_token)
        try:
            self.storageFileview = CONFIG["synapse"]["master_fileview"]

            # get data in administrative fileview for this pipeline
            self.storageFileviewTable = self.syn.tableQuery(
                "SELECT * FROM " + self.storageFileview
            ).asDataFrame()

            self.manifest = CONFIG["synapse"]["manifest_basename"]
        
        except KeyError:
            raise MissingConfigValueError(("synapse", "master_fileview"))
        except AttributeError:
            raise AttributeError("storageFileview attribute has not been set.")
        except SynapseHTTPError:
            raise AccessCredentialsError(self.storageFileview)
        except ValueError:
            raise MissingConfigValueError(("synapse", "master_fileview"))

    @staticmethod
    def login(token=None, access_token=None, input_token=None):
        # If no token is provided, try retrieving access token from environment
        if not token and not access_token and not input_token:
            access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")

        # login using a token
        if token:
            syn = synapseclient.Synapse()

            try:
                syn.login(sessionToken=token, silent=True)
            except synapseclient.core.exceptions.SynapseHTTPError:
                raise ValueError("Please make sure you are logged into synapse.org.")
        elif access_token:
            syn = synapseclient.Synapse()
            syn.default_headers["Authorization"] = f"Bearer {access_token}"
        elif input_token: 
            try: 
                syn = synapseclient.Synapse()
                syn.default_headers["Authorization"] = f"Bearer {input_token}"
            except synapseclient.core.exceptions.SynapseHTTPError:
                raise ValueError("No access to resources. Please make sure that your token is correct")
        else:
            # login using synapse credentials provided by user in .synapseConfig (default) file
            syn = synapseclient.Synapse(configPath=CONFIG.SYNAPSE_CONFIG_PATH)
            syn.login(silent=True)
            
        return syn

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

    def getStorageProjects(self) -> List[str]:
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
        walked_path = synapseutils.walk(self.syn, datasetId)

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

    def getDatasetManifest(
        self, datasetId: str, downloadFile: bool = False
    ) -> List[str]:
        """Gets the manifest associated with a given dataset.

        Args:
            datasetId: synapse ID of a storage dataset.
            downloadFile: boolean argument indicating if manifest file in dataset should be downloaded or not.

        Returns:
            manifest_syn_id (String): Synapse ID of exisiting manifest file.
            manifest_data (synapseclient.entity.File): Synapse entity if downloadFile is True.
            "" (String): No pre-exisiting manifest in dataset.
        """

        # get a list of files containing the manifest for this dataset (if any)
        all_files = self.storageFileviewTable

        manifest_re=re.compile(os.path.basename(self.manifest)+".*.[tc]sv")
        manifest = all_files[
            (all_files['name'].str.contains(manifest_re,regex=True))
            & (all_files["parentId"] == datasetId)
        ]

        manifest = manifest[["id", "name"]]
        censored_regex=re.compile('.*censored.*')
        
        # if there is no pre-exisiting manifest in the specified dataset
        if manifest.empty:
            return ""

        # if there is an exisiting manifest
        else:
            # retrieve data from synapse

            # if a censored manifest exists for this dataset
            censored = manifest['name'].str.contains(censored_regex)
            if any(censored):
                # Try to use uncensored manifest first
                not_censored=~censored
                if any(not_censored):
                    manifest_syn_id=manifest[not_censored]["id"][0]

            #otherwise, use the first (implied only) version that exists
            else:
                manifest_syn_id = manifest["id"][0]


            # if the downloadFile option is set to True
            if downloadFile:
                # enables retrying if user does not have access to uncensored manifest
                while True:
                    # pass synID to synapseclient.Synapse.get() method to download (and overwrite) file to a location
                    try:
                        if 'manifest_folder' in CONFIG['synapse'].keys():
                            manifest_data = self.syn.get(
                                manifest_syn_id,
                                downloadLocation=CONFIG["synapse"]["manifest_folder"],
                                ifcollision="overwrite.local",
                            )
                            break
                        # if no manifest folder is set, download to cache
                        else:
                            manifest_data = self.syn.get(
                                manifest_syn_id,
                            )
                            break
                    # If user does not have access to uncensored manifest, use censored instead
                    except(SynapseUnmetAccessRestrictions):
                            manifest_syn_id=manifest[censored]["id"][0]
                        

                return manifest_data


            return manifest_syn_id

    def updateDatasetManifestFiles(self, datasetId: str, store:bool = True) -> Union[Tuple[str, pd.DataFrame], None]:
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
                manifest_id = self.associateMetadataWithFiles(manifest_filepath, datasetId)

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
                if 'Component' in annotations:
                    component = annotations['Component']
                    entity = self.syn.get(manifestId, downloadFile=False)
                    manifest_name = entity["properties"]["name"]

                # otherwise download the manifest and parse for information
                elif 'Component' not in annotations or not annotations:
                    logging.debug(
                        f"No component annotations have been found for manifest {manifestId}. "
                        "The manifest will be downloaded and parsed instead. "
                        "For increased speed, add component annotations to manifest."
                        )

                    manifest_info = self.getDatasetManifest(datasetId,downloadFile=True)
                    manifest_name = manifest_info["properties"]["name"]
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

            #save manifest list with applicable informaiton
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
                manifest_table_id = upload_format_manifest_table(manifest, dataset_id, datasetName)
                manifest_loaded.append(datasetName)
        return manifest_loaded

    def get_synapse_table(self, synapse_id: str) -> Tuple[pd.DataFrame, CsvFileTable]:
        """Download synapse table as a pd dataframe; return table schema and etags as results too

        Args:
            synapse_id: synapse ID of the table to query
        """

        results = self.syn.tableQuery("SELECT * FROM {}".format(synapse_id))
        df = results.asDataFrame(rowIdAndVersionInIndex=False)

        return df, results

    def upload_format_manifest_table(self, se, manifest, datasetId, table_prefix, restrict):
        # Rename the manifest columns to display names to match fileview
        blacklist_chars = ['(', ')', '.', ' ']
        manifest_columns = manifest.columns.tolist()

        cols = [
            se.get_class_label_from_display_name(
                str(col)
                ).translate({ord(x): '' for x in blacklist_chars})
            for col in manifest_columns
        ]

        cols = list(map(lambda x: x.replace('EntityId', 'entityId'), cols))


        # Reset column names in manifest
        manifest.columns = cols

        #move entity id to end of df
        entity_col = manifest.pop('entityId')
        manifest.insert(len(manifest.columns), 'entityId', entity_col)

        # Get the column schema
        col_schema = as_table_columns(manifest)

        # Set uuid column length to 64 (for some reason not being auto set.)
        for i, col in enumerate(col_schema):
            if col['name'] == 'Uuid':
                col_schema[i]['maximumSize'] = 64

        # Put manifest onto synapse
        schema = Schema(name=table_prefix + '_manifest_table', columns=col_schema, parent=datasetId)
        table = self.syn.store(Table(schema, manifest), isRestricted=restrict)
        manifest_table_id = table.schema.id

        return manifest_table_id, manifest

    def uplodad_manifest_file(self, manifest, metadataManifestPath, datasetId, restrict_manifest):
        # Update manifest to have the new entityId column
        manifest.to_csv(metadataManifestPath, index=False)

        # store manifest to Synapse as a CSV
        manifestSynapseFile = File(
            metadataManifestPath,
            description="Manifest for dataset " + datasetId,
            parent=datasetId,
        )

        manifest_synapse_file_id = self.syn.store(manifestSynapseFile, isRestricted = restrict_manifest).id
        
        return manifest_synapse_file_id

    def format_row_annotations(self, se, row, entityId, useSchemaLabel, hideBlanks):
        # prepare metadata for Synapse storage (resolve display name into a name that Synapse annotations support (e.g no spaces, parenthesis)
        # note: the removal of special characters, will apply only to annotation keys; we are not altering the manifest
        # this could create a divergence between manifest column and annotations. this should be ok for most use cases.
        # columns with special characters are outside of the schema
        metadataSyn = {}
        blacklist_chars = ['(', ')', '.', ' ']
        
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

        for anno_k, anno_v in metadataSyn.items():
            
            #Do not save blank annotations as NaNs,
            #remove keys with nan/blank values from dict of annotations to be uploaded if present on current data annotation
            if isinstance(anno_v,float) and np.isnan(anno_v):
                if hideBlanks:
                    annos.pop(anno_k) if anno_k in annos.keys() else annos
                else:
                    annos[anno_k] = ""
            else:
                annos[anno_k] = anno_v
        return annos

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

    def associateMetadataWithFiles(
        self, metadataManifestPath: str, datasetId: str, manifest_record_type: str = 'both', 
        useSchemaLabel: bool = True, hideBlanks: bool = False, restrict_manifest = False,
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
            metadataManifestPath: path to csv containing a validated metadata manifest.
            The manifest should include a column entityId containing synapse IDs of files/entities to be associated with metadata, if that is applicable to the dataset type.
            Some datasets, e.g. clinical data, do not contain file id's, but data is stored in a table: one row per item.
            In this case, the system creates a file on Synapse for each row in the table (e.g. patient, biospecimen) and associates the columnset data as metadata/annotations to his file.
            datasetId: synapse ID of folder containing the dataset
            useSchemaLabel: Default is True - use the schema label. If False, uses the display label from the schema. Attribute display names in the schema must not only include characters that are not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'.
            manifest_record_type: valid values are 'entity', 'table' or 'both'. Specifies whether to create entity ids and folders for each row in a manifest, a Synapse table to house the entire manifest or do both.
            hideBlanks: Default is false. Boolean flag that does not upload annotation keys with blank values when true. Uploads Annotation keys with empty string values when false.
        Returns:
            manifest_synapse_file_id: SynID of manifest csv uploaded to synapse.

        Raises:
            ValueError: manifest_record_type is not 'entity', 'table' or 'both'
            FileNotFoundError: Manifest file does not exist at provided path.

        """

        # Check that record type provided matches expected input.
        manifest_record_types = ['entity', 'table', 'both']
        try:
            manifest_record_type in manifest_record_types
        except ValueError as err:
            raise ValueError(
                f"manifest_record_type provided: {manifest_record_type}, is not one of the accepted "
                f"types: {manifest_record_types}"
            ) from err

        # read new manifest csv
        try:
            kwargs={
                "dtype":"string"
            }
            manifest = load_df(metadataManifestPath, preserve_raw_input=False, **kwargs)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"No manifest file was found at this path: {metadataManifestPath}"
            ) from err

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

        # If specified, upload manifest as a table and get the SynID and manifest
        if manifest_record_type == 'table' or manifest_record_type == 'both':
            manifest_synapse_table_id, manifest = self.upload_format_manifest_table(
                                                        se, manifest, datasetId, manifest['Component'][0].lower(), restrict = restrict_manifest)
        # Iterate over manifest rows, create Synapse entities and store corresponding entity IDs in manifest if needed
        # also set metadata for each synapse entity as Synapse annotations
        for idx, row in manifest.iterrows():
            if not row["entityId"] and (manifest_record_type == 'entity' or 
                manifest_record_type == 'both'):
                # no entity exists for this row
                # so create one
                rowEntity = Folder(str(uuid.uuid4()), parent=datasetId)
                rowEntity = self.syn.store(rowEntity)
                entityId = rowEntity["id"]
                row["entityId"] = entityId
                manifest.loc[idx, "entityId"] = entityId
            elif not row["entityId"] and manifest_record_type == 'table':
                # If not using entityIds, fill with manifest_table_id so 
                row["entityId"] = manifest_synapse_table_id
                entityId = ''
            else:
                # get the entity id corresponding to this row
                entityId = row["entityId"]

            # Adding annotations to connected files.
            if entityId:
                # Format annotations for Synapse
                annos = self.format_row_annotations(se, row, entityId, useSchemaLabel, hideBlanks)

                # Store annotations for an entity folder
                self.syn.set_annotations(annos)

        # Load manifest to synapse as a CSV File
        manifest_synapse_file_id = self.uplodad_manifest_file(manifest, metadataManifestPath, datasetId, restrict_manifest)
        
        # Get annotations for the file manifest.
        manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_file_id)
        
        self.syn.set_annotations(manifest_annotations)

        logger.info("Associated manifest file with dataset on Synapse.")
            
        if manifest_record_type == 'table' or manifest_record_type == 'both':
            # Update manifest Synapse table with new entity id column.
            self.make_synapse_table(
                table_to_load = manifest,
                dataset_id = datasetId,
                existingTableId = manifest_synapse_table_id,
                table_name = manifest['Component'][0].lower() + '_manifest_table',
                update_col = 'Uuid',
                specify_schema = False,
                )
            
            # Get annotations for the table manifest
            manifest_annotations = self.format_manifest_annotations(manifest, manifest_synapse_table_id)
            self.syn.set_annotations(manifest_annotations)

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
            raise ValueError(
                f"The given dataset ({datasetId}) isn't accessible with this "
                "user. This might be caused by a typo in the dataset Synapse ID."
            )

        # If not, then assume dataset not in file view
        raise ValueError(
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

    def make_synapse_table(self, table_to_load, dataset_id, existingTableId, table_name, 
            update_col = 'entityId', column_type_dictionary = {}, specify_schema=True):
        '''
        Record based data
        '''
        # create/update a table corresponding to this dataset in this dataset's parent project
        # update_col is the column in the table that has a unique code that will allow Synapse to
        # locate its position in the old and new table.
        if existingTableId:
            existing_table, existing_results = self.get_synapse_table(existingTableId)
            table_to_load = update_df(existing_table, table_to_load, update_col)
            self.syn.store(Table(existingTableId, table_to_load, etag = existing_results.etag))
            # remove system metadata from manifest
            existing_table.drop(columns = ['ROW_ID', 'ROW_VERSION'], inplace = True)
        else:
            datasetEntity = self.syn.get(dataset_id, downloadFile = False)
            datasetName = datasetEntity.name
            if not table_name:
                table_name = datasetName + 'table'
            datasetParentProject = self.getDatasetProject(dataset_id)
            if specify_schema:
                if column_type_dictionary == {}:
                    logger.error("Did not provide a column_type_dictionary.")
                #create list of columns:
                cols = []
                for col in table_to_load.columns:
                    if col in column_type_dictionary:
                        col_type = column_type_dictionary[col]['column_type']
                        max_size = column_type_dictionary[col]['maximum_size']
                        max_list_len = column_type_dictionary[col]['maximum_list_length']
                        if max_size and max_list_len:
                            cols.append(Column(name=col, columnType=col_type, 
                                maximumSize=max_size, maximumListLength=max_list_len))
                        elif max_size:
                            cols.append(Column(name=col, columnType=col_type, 
                                maximumSize=max_size))
                        else:
                            cols.append(Column(name=col, columnType=col_type))
                    else:
                        cols.append(Column(name=col, columnType='STRING', maximumSize=500))
                schema = Schema(name=table_name, columns=cols, parent=datasetParentProject)
                table = Table(schema, table_to_load)
                table_id = self.syn.store(table)
                return table.schema.id
            else:
                # For just uploading the tables to synapse using default
                # column types.
                table = build_table(table_name, datasetParentProject, table_to_load)
                table = self.syn.store(table)
                return table.schema.id


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
