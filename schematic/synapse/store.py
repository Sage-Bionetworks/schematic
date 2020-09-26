# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text, List

import os

# used to generate unique names for entities
import uuid

# manipulation of dataframes 
import pandas as pd

# Python client for Synapse
import synapseclient

from synapseclient import File, Folder, Table
from synapseclient.table import build_table
import synapseutils

from schematic.utils.df_utils import update_df
from schematic.schemas.explorer import SchemaExplorer

from schematic.utils.config_utils import load_yaml
from definitions import ROOT_DIR, CONFIG_PATH, DATA_PATH

config_data = load_yaml(CONFIG_PATH)

class SynapseStorage(object):
    """Implementation of Storage interface for datasets/files stored on Synapse.

    Provides utilities to list files in a specific project; update files annotations, create fileviews, etc.

    TODO: Need to define the interface and rename and/or refactor some of the methods below.
    """

    def __init__(self,
                syn: synapseclient = None,
                token: str = None # optional parameter retreived from browser cookie
                ) -> None:

        """Initializes a SynapseStorage object.

        Args:
            syn: an object of type synapseclient.
            token: optional token parameter (typically a 'str') as found in browser cookie upon login to synapse.

            TODO: move away from specific project setup and work with an interface that Synapse specifies (e.g. based on schemas).

        Exceptions:
            KeyError: when the 'storage' config object is missing values for essential keys.
            AttributeError: when the 'storageFileview' attribute (of class SynapseStorage) does not have a value associated with it.
            synapseclient.core.exceptions.SynapseHTTPError: check if the current user has permission to access the Synapse entity.
            ValueError: when Admin fileview cannot be found (describe further).

        Typical usage example:
            syn_store = SynapseStorage(syn=syn)

            where 'syn' is an object of type synapseclient.
        """
     
        # login using a token 
        if token:
            self.syn = synapseclient.Synapse()

            try:
                self.syn.login(sessionToken = token)
            except synapseclient.core.exceptions.SynapseHTTPError:
                print("Please enter a valid session token.")
                return
        elif syn: # if no token, assume a logged in synapseclient instance has been provided
            if isinstance(syn, synapseclient.Synapse):
                self.syn = syn
            else:
                print("Please make sure 'syn' argument is of type synapseclient.Synapse().")
                return

        try:
            self.storageFileview = config_data["synapse"]["master_fileview"]

            # get data in administrative fileview for this pipeline
            self.storageFileviewTable = self.syn.tableQuery("SELECT * FROM " + self.storageFileview).asDataFrame()

            self.manifest = os.path.join(DATA_PATH, config_data["synapse"]["manifest_filename"])
        except KeyError as key_exc:
            print("Missing value(s) for the {} key(s) in the config file.".format(key_exc))
        except AttributeError:
            print("'storageFileview' attribute does not have a value.")
        except synapseclient.core.exceptions.SynapseHTTPError:
            print("Check if you have ACCESS to project: {}.".format(self.storageFileview))
        except ValueError:
            print("Administrative Fileview {} not found.".format(self.storageFileview))


    def getPaginatedRestResults(self, currentUserId : str) -> Dict[str, str]:
        """Gets the paginated results of the REST call to Synapse to check what projects the current user has access to.

        Args:
            currentUserId: synapse id for the user whose projects we want to get. 
            
        Returns: 
            A dictionary with a next page token and the results.
        """
        all_results = self.syn.restGET('/projects/user/{principalId}'.format(principalId=currentUserId))
    
        while 'nextPageToken' in all_results: # iterate over next page token in results while there is any
            results_token = self.syn.restGET('/projects/user/{principalId}?nextPageToken={nextPageToken}'.format(principalId=currentUserId, nextPageToken = all_results['nextPageToken']))
            all_results['results'].extend(results_token['results'])

            if 'nextPageToken' in results_token:
                all_results['nextPageToken'] = results_token['nextPageToken']
            else:
                del(all_results['nextPageToken'])

        return all_results


    def getStorageProjects(self) -> List[str]:
        """Gets all storage projects the current user has access to, within the scope of the 'storageFileview' attribute.

        Returns: 
            A list of storage projects the current user has access to; the list consists of tuples (projectId, projectName).
        """

        # get the set of all storage Synapse project accessible for this pipeline
        if hasattr(self, 'storageFileviewTable'):
            storageProjects = self.storageFileviewTable["projectId"].unique()
        else:
            print("'storageFileviewTable' attribute value is missing.")

        # get the set of storage Synapse project accessible for this user

        # get current user name and user ID
        currentUser = self.syn.getUserProfile()
        currentUserName = currentUser.userName
        currentUserId = currentUser.ownerId
        
        # get a list of projects from Synapse
        currentUserProjects = self.getPaginatedRestResults(currentUserId)
        
        # prune results json filtering project id
        currentUserProjects = [currentUserProject.get('id') for currentUserProject in currentUserProjects["results"]]

        # find set of user projects that are also in this pipeline's storage projects set
        storageProjects = list(set(storageProjects) & set(currentUserProjects))
    
        # prepare a return list of project IDs and names
        projects = []
        for projectId in storageProjects:
            projectName = self.syn.get(projectId, downloadFile = False).name
            projects.append((projectId, projectName))

        sorted_projects_list = sorted(projects, key=lambda tup: tup[0])

        return sorted_projects_list


    def getStorageDatasetsInProject(self, projectId: str) -> List[str]:
        """Gets all datasets in folder under a given storage project that the current user has access to.

        Args:
            projectId: synapse ID of a storage project.

        Returns: 
            A list of datasets within the given storage project; the list consists of tuples (datasetId, datasetName).
        
        Raises:
            ValueError: Project ID not found.
        """
        
        # select all folders and fetch their names from within the storage project; 
        # if folder content type is defined, only select folders that contain datasets
        areDatasets = False
        if "contentType" in self.storageFileviewTable.columns:
            foldersTable = self.storageFileviewTable[(self.storageFileviewTable["contentType"] == "dataset") & (self.storageFileviewTable["projectId"] == projectId)]
            areDatasets = True
        else:
            foldersTable = self.storageFileviewTable[(self.storageFileviewTable["type"] == "folder") & (self.storageFileviewTable["projectId"] == projectId)]

        # get an array of tuples (folderId, folderName)
        # some folders are part of datasets; others contain datasets
        # each dataset parent is the project; folders part of a dataset have another folder as a parent
        # to get folders if and only if they contain datasets for each folder 
        # check if folder's parent is the project; if so that folder contains a dataset,
        # unless the folder list has already been filtered to dataset folders based on contentType attribute above
        
        datasetList = []
        folderProperties = ["id", "name"]
        for folder in list(foldersTable[folderProperties].itertuples(index = False, name = None)):
            try:
                if self.syn.get(folder[0], downloadFile = False).properties["parentId"] == projectId or areDatasets:
                    datasetList.append(folder)
            except ValueError:
                print("The project id {} was not found.".format(projectId))

        sorted_dataset_list = sorted(datasetList, key=lambda tup: tup[0])

        return sorted_dataset_list


    def getFilesInStorageDataset(self, datasetId: str, fileNames: List = None) -> List[str]:
        """Gets all files in a given dataset folder.

        Args:
            datasetId: synapse ID of a storage dataset.
            fileNames: get a list of files with particular names; defaults to None in which case all dataset files are returned (except bookkeeping files, e.g.
            metadata manifests); if fileNames is not None, all files matching the names in the fileNames list are returned if present.
        
        Returns: a list of files; the list consists of tuples (fileId, fileName).
        
        Raises:
            ValueError: Dataset ID not found.
        """

        # select all files within a given storage dataset (top level folder in a Synapse storage project)
        filesTable = self.storageFileviewTable[(self.storageFileviewTable["type"] == "file") & (self.storageFileviewTable["parentId"] == datasetId)]

        # return a list of tuples (fileId, fileName)
        fileList = []
        for row in filesTable[["id", "name"]].itertuples(index = False, name = None):
            # if not row[1] == self.manifest and not fileNames:
            if not "manifest" in row[1] and not fileNames:
                # check if a metadata-manifest file has been passed in the list of filenames; assuming the manifest file has a specific filename, e.g. synapse_storage_manifest.csv; remove the manifest filename if so; (no need to add metadata to the metadata container); TODO: expose manifest filename as a configurable parameter and don't hard code.
                fileList.append(row)

            elif not fileNames == None and row[1] in fileNames:
                # if fileNames is specified and file is in fileNames add it to the returned list
                fileList.append(row)

        sorted_files_list = sorted(fileList, key=lambda tup: tup[0])

        return sorted_files_list
        

    def getDatasetManifest(self, datasetId: str) -> List[str]:
        """Gets the manifest associated with a given dataset.

        Args:
            datasetId: synapse ID of a storage dataset.
        
        Returns: a tuple of manifest file ID and manifest name -- (fileId, fileName); returns empty list if no manifest is found.
        """

        # get a list of files containing the manifest for this dataset (if any)
        manifest = self.getFilesInStorageDataset(datasetId, fileNames = [os.path.basename(self.manifest)])
        
        if not manifest:
            return []
        else:
            return manifest[0] # extract manifest tuple from list


    def updateDatasetManifestFiles(self, datasetId: str) -> str:
        """Fetch the names and entity IDs of all current files in dataset in store, if any; update dataset's manifest with new files, if any.

        Args:
            datasetId: synapse ID of a storage dataset.
        
        Returns: synapse ID of updated manifest.
        """

        # get existing manifest Synapse ID
        manifest_id_name = self.getDatasetManifest(datasetId)
        if not manifest_id_name:
            # no manifest exists yet: abort
            print("No manifest found in storage dataset " + datasetId + "! Abort.")
            return ""

        manifest_id = manifest_id_name[0]
        manifest_filepath = self.syn.get(manifest_id).path
        manifest = pd.read_csv(manifest_filepath)

        # get current list of files
        dataset_files = self.getFilesInStorageDataset(datasetId)

        # update manifest with additional filenames, if any
        # note that if there is an existing manifest and there are files in the dataset 
        # the columns Filename and entityId are assumed to be present in manifest schema
        # TODO: use idiomatic panda syntax
        if dataset_files:
            new_files = {
                    "Filename": [],
                    "entityId": []
            }

            # find new files if any
            for file_id, file_name in dataset_files:
                if not file_id in manifest["entityId"].values:
                    new_files["Filename"].append(file_name)
                    new_files["entityId"].append(file_id)
            
            # update manifest so that it contain new files
            #manifest = pd.DataFrame(new_files)
            new_files = pd.DataFrame(new_files)
            manifest = pd.concat([new_files, manifest], sort = False).reset_index().drop("index", axis = 1)
            # update the manifest file, so that it contains the relevant entity IDs
            manifest.to_csv(manifest_filepath, index = False)

            # store manifest and update associated metadata with manifest on Synapse
            manifest_id = self.associateMetadataWithFiles(manifest_filepath, datasetId)
            
        return manifest_id


    def getAllManifests(self) -> List[str]:
        """Gets all metadata manifest files across all datasets in projects a user has access to.

        Returns: a list of projects, datasets per project and metadata manifest Synapse ID for each dataset
                 as a list of tuples, one for each manifest:
                    [
                        (
                            (projectId, projectName),
                            (datasetId, dataName),
                            (manifestId, manifestName)
                        ),
                        ...
                    ]

        TODO: return manifest URI instead of Synapse ID for interoperability with other implementations of a store interface
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
                            self.getDatasetManifest(datasetId)
                )
                manifests.append(manifest)

        return manifests


    def associateMetadataWithFiles(self, metadataManifestPath: str, datasetId: str) -> str:
        """Associate metadata with files in a storage dataset already on Synapse. 
        Upload metadataManifest in the storage dataset folder on Synapse as well. Return synapseId of the uploaded manifest file.
        
        Args: 
            metadataManifestPath: path to csv containing a validated metadata manifest. 
            The manifest should include a column entityId containing synapse IDs of files/entities to be associated with metadata, if that is applicable to the dataset type. 
            Some datasets, e.g. clinical data, do not contain file id's, but data is stored in a table: one row per item. 
            In this case, the system creates a file on Synapse for each row in the table (e.g. patient, biospecimen) and associates the columnset data as metadata/annotations to his file. 
                
            datasetId: synapse ID of folder containing the dataset
            
        Returns: synapse Id of the uploaded manifest.
        
        Raises: TODO
            FileNotFoundException: Manifest file does not exist at provided path.
        """

        # determine dataset name
        datasetEntity = self.syn.get(datasetId, downloadFile = False)
        datasetName = datasetEntity.name
        datasetParentProject = datasetEntity.properties["parentId"]

        # read new manifest csv
        try:
            manifest = pd.read_csv(metadataManifestPath)
        except FileNotFoundError:
            print("No mainfest file was found at this path: {}.".format(metadataManifestPath))

        # check if there is an existing manifest
        existingManifest = self.getDatasetManifest(datasetId)
        existingTableId = None

        if existingManifest:

            # update the existing manifest, so that existing entities get updated metadata and new entities are preserved; 
            # note that an existing manifest always contains an entityId column, which is assumed to be the index key
            # if updating an existing manifest the new manifest should also contain an entityId column 
            # (it is ok if the entities ID in the new manifest are blank)
            manifest['entityId'].fillna('', inplace = True)
            manifest = update_df(manifest, existingManifest, "entityId")

            # retrieve Synapse table associated with this manifest, so that it can be updated below
            existingTableId = self.syn.findEntityId(datasetName + "_table", datasetParentProject)
            
        # if this is a new manifest there could be no Synapse entities associated with the rows of this manifest
        # this may be due to data type (e.g. clinical data) being tabular 
        # and not requiring files; to utilize uniform interfaces downstream
        # (i.e. fileviews), a Synapse entity (a folder) is created for each row 
        # and an entity column is added to the manifest containing the resulting 
        # entity IDs; a table is also created at present as an additional interface 
        # for downstream query and interaction with the data. 
        
        if not "entityId" in manifest.columns:
            manifest["entityId"] = ""

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

                metadataSyn[keySyn] = v

            # set annotation(s) for the various objects/items in a dataset on Synapse
            annos = self.syn.get_annotations(entityId)

            for anno_k, anno_v in metadataSyn.items():
                annos[anno_k] = metadataSyn[anno_k]

            self.syn.set_annotations(annos)
            #self.syn.set_annotations(metadataSyn) #-- deprecated code

        # create/update a table corresponding to this dataset in this dataset's parent project

        if existingTableId:
            # if table already exists, delete it and upload the new table
            # TODO: do a proper Synapse table update
            self.syn.delete(existingTableId)
        
        # create table using latest manifest content
        table = build_table(datasetName + "_table", self.syn.get(datasetId, downloadFile = False).properties["parentId"], manifest)
        table = self.syn.store(table)
         
        # update the manifest file, so that it contains the relevant entity IDs
        manifest.to_csv(metadataManifestPath, index = False)

        # store manifest to Synapse
        manifestSynapseFile = File(metadataManifestPath, description = "Manifest for dataset " + datasetId, parent = datasetId)

        manifestSynapseFileId = self.syn.store(manifestSynapseFile).id

        return manifestSynapseFileId
