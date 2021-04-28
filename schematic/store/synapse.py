import os
import uuid  # used to generate unique names for entities
import json
import atexit
import logging
import secrets

# allows specifying explicit variable types
from typing import Dict, List, Tuple, Sequence
from collections import OrderedDict

import numpy as np
import pandas as pd
import synapseclient
import synapseutils

from synapseclient import (
    Synapse,
    File,
    Folder,
    EntityViewSchema,
    EntityViewType,
    Column,
)
from synapseclient.table import CsvFileTable
from synapseclient.annotations import from_synapse_annotations
from synapseclient.core.exceptions import SynapseHTTPError, SynapseAuthenticationError
import synapseutils

from schematic.utils.df_utils import update_df
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

        # If no token is provided, try retrieving access token from environment
        if not token and not access_token:
            access_token = os.getenv("SYNAPSE_ACCESS_TOKEN")

        # login using a token
        if token:
            self.syn = synapseclient.Synapse()

            try:
                self.syn.login(sessionToken=token, silent=True)
            except synapseclient.core.exceptions.SynapseHTTPError:
                raise ValueError("Please make sure you are logged into synapse.org.")
        elif access_token:
            self.syn = synapseclient.Synapse()
            self.syn.default_headers["Authorization"] = f"Bearer {access_token}"
        else:
            # login using synapse credentials provided by user in .synapseConfig (default) file
            self.syn = synapseclient.Synapse(configPath=CONFIG.SYNAPSE_CONFIG_PATH)
            self.syn.login(silent=True)

        try:
            self.storageFileview = CONFIG["synapse"]["master_fileview"]

            # get data in administrative fileview for this pipeline
            self.storageFileviewTable = self.syn.tableQuery(
                "SELECT * FROM " + self.storageFileview
            ).asDataFrame()

            self.manifest = CONFIG["synapse"]["manifest_filename"]
        except KeyError:
            raise MissingConfigValueError(("synapse", "master_fileview"))
        except AttributeError:
            raise AttributeError("storageFileview attribute has not been set.")
        except SynapseHTTPError:
            raise AccessCredentialsError(self.storageFileview)
        except ValueError:
            raise MissingConfigValueError(("synapse", "master_fileview"))

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
                    not fileNames == None and filename[0] in fileNames
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
        manifest = all_files[
            (all_files["name"] == os.path.basename(self.manifest))
            & (all_files["parentId"] == datasetId)
        ]
        manifest = manifest[["id", "name"]]

        # if there is no pre-exisiting manifest in the specified dataset
        if manifest.empty:
            return ""
        # if there is an exisiting manifest
        else:
            # if the downloadFile option is set to True
            if downloadFile:
                # retrieve data from synapse
                manifest_syn_id = manifest["id"][0]

                # pass synID to synapseclient.Synapse.get() method to download (and overwrite) file to a location
                manifest_data = self.syn.get(
                    manifest_syn_id,
                    downloadLocation=CONFIG["synapse"]["manifest_folder"],
                    ifcollision="overwrite.local",
                )

                return manifest_data

            # extract synapse ID of exisiting dataset manifest
            manifest_syn_id = manifest.to_records(index=False)[0][0]

            return manifest_syn_id

    def updateDatasetManifestFiles(self, datasetId: str) -> str:
        """Fetch the names and entity IDs of all current files in dataset in store, if any; update dataset's manifest with new files, if any.

        Args:
            datasetId: synapse ID of a storage dataset.

        Returns:
            Synapse ID of updated manifest.
        """

        # get existing manifest Synapse ID
        manifest_id = self.getDatasetManifest(datasetId)
        if not manifest_id:
            # no manifest exists yet: abort
            raise FileNotFoundError(
                f"Manifest file {CONFIG['synapse']['manifest_filename']} "
                f"cannot be found in {datasetId} dataset folder."
            )

        manifest_filepath = self.syn.get(manifest_id).path
        manifest = pd.read_csv(manifest_filepath)

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
            # manifest = pd.DataFrame(new_files)
            new_files = pd.DataFrame(new_files)
            manifest = (
                pd.concat([new_files, manifest], sort=False)
                .reset_index()
                .drop("index", axis=1)
            )
            # update the manifest file, so that it contains the relevant entity IDs
            manifest.to_csv(manifest_filepath, index=False)

            # store manifest and update associated metadata with manifest on Synapse
            manifest_id = self.associateMetadataWithFiles(manifest_filepath, datasetId)

        return manifest_id

    def getAllManifests(self) -> List[str]:
        """Gets all metadata manifest files across all datasets in projects a user has access to.

        Returns: A list of projects, datasets per project and metadata manifest Synapse ID for each dataset
                 as a list of tuples, one for each manifest:
                    [
                        (
                            (projectId, projectName),
                            (datasetId, dataName),
                            (manifestId, manifestName)
                        ),
                        ...
                    ]

        TODO: Return manifest URI instead of Synapse ID for interoperability with other implementations of a store interface
        TODO: Use fileview instead of iterating through projects and datasets
        TODO: GetDatasetManifest() return type has changed to return only manifest synapse ID. Fetch
              manifestName from config.yml and create tuple
        """

        projects = self.getStorageProjects()

        manifests = []
        for projectId, projectName in projects:

            datasets = self.getStorageDatasetsInProject(projectId)

            for (datasetId, datasetName) in datasets:

                # encode information about the manifest in a simple list (so that R clients can unpack it)
                # eventually can serialize differently
                manifest = (
                    (projectId, projectName),
                    (datasetId, datasetName),
                    self.getDatasetManifest(datasetId),
                )

                manifests.append(manifest)

        return manifests

    def get_synapse_table(self, synapse_id: str) -> Tuple[pd.DataFrame, CsvFileTable]:
        """Download synapse table as a pd dataframe; return table schema and etags as results too

        Args:
            synapse_id: synapse ID of the table to query
        """

        results = self.syn.tableQuery("SELECT * FROM {}".format(synapse_id))
        df = results.asDataFrame(rowIdAndVersionInIndex=False)

        return df, results

    def associateMetadataWithFiles(
        self, metadataManifestPath: str, datasetId: str
    ) -> str:
        """Associate metadata with files in a storage dataset already on Synapse.
        Upload metadataManifest in the storage dataset folder on Synapse as well. Return synapseId of the uploaded manifest file.

        Args:
            metadataManifestPath: path to csv containing a validated metadata manifest.
            The manifest should include a column entityId containing synapse IDs of files/entities to be associated with metadata, if that is applicable to the dataset type.
            Some datasets, e.g. clinical data, do not contain file id's, but data is stored in a table: one row per item.
            In this case, the system creates a file on Synapse for each row in the table (e.g. patient, biospecimen) and associates the columnset data as metadata/annotations to his file.
            datasetId: synapse ID of folder containing the dataset

        Returns:
            Synapse Id of the uploaded manifest.

        Raises:
            FileNotFoundError: Manifest file does not exist at provided path.
        """

        # determine dataset name
        # datasetEntity = self.syn.get(datasetId, downloadFile = False)
        # datasetName = datasetEntity.name
        # datasetParentProject = self.getDatasetProject(datasetId)

        # read new manifest csv
        try:
            manifest = pd.read_csv(metadataManifestPath)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"No manifest file was found at this path: {metadataManifestPath}"
            ) from err

        # if this is a new manifest there could be no Synapse entities associated with the rows of this manifest
        # this may be due to data type (e.g. clinical data) being tabular
        # and not requiring files; to utilize uniform interfaces downstream
        # (i.e. fileviews), a Synapse entity (a folder) is created for each row
        # and an entity column is added to the manifest containing the resulting
        # entity IDs; a table is also created at present as an additional interface
        # for downstream query and interaction with the data.

        if not "entityId" in manifest.columns:
            manifest["entityId"] = ""
        else:
            manifest["entityId"].fillna("", inplace=True)

        # get a schema explorer object to ensure schema attribute names used in manifest are translated to schema labels for synapse annotations
        se = SchemaExplorer()

        # iterate over manifest rows, create Synapse entities and store corresponding entity IDs in manifest if needed
        # also set metadata for each synapse entity as Synapse annotations
        for idx, row in manifest.iterrows():
            if not row["entityId"]:
                # no entity exists for this row
                # so create one
                rowEntity = Folder(str(uuid.uuid4()), parent=datasetId)
                rowEntity = self.syn.store(rowEntity)
                entityId = rowEntity["id"]
                row["entityId"] = entityId
                manifest.loc[idx, "entityId"] = entityId
            else:
                # get the entity id corresponding to this row
                entityId = row["entityId"]

            #  prepare metadata for Synapse storage (resolve display name into a name that Synapse annotations support (e.g no spaces)
            metadataSyn = {}
            for k, v in row.to_dict().items():
                keySyn = se.get_class_label_from_display_name(str(k))

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
                annos[anno_k] = metadataSyn[anno_k]

            self.syn.set_annotations(annos)
            # self.syn.set_annotations(metadataSyn) #-- deprecated code

        # update the manifest file, so that it contains the relevant entity IDs
        manifest.to_csv(metadataManifestPath, index=False)

        # store manifest to Synapse
        manifestSynapseFile = File(
            metadataManifestPath,
            description="Manifest for dataset " + datasetId,
            parent=datasetId,
        )
        logger.info("Associated manifest file with dataset on Synapse.")

        manifestSynapseFileId = self.syn.store(manifestSynapseFile).id

        return manifestSynapseFileId

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
