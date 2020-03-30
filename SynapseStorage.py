# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

# manipulation of dataframes 
import pandas as pd

# Python client for Synapse
import synapseclient

from synapseclient import File, Folder
from synapseclient.table import build_table

from schema_explorer import SchemaExplorer 

import synapseutils

class SynapseStorage(object):

    """Implementation of Storage interface for datasets/files stored on Synapse.
    Provides utilities to list files in a specific project; update files annotations, create fileviews, etc.
    TODO: Need to define the interface and rename and/or refactor some of the methods below.
    """


    def __init__(self,
                 storageFileview: str,
                 syn: synapseclient = None,
                 token: str = None ## gets sessionToken for logging in
                 ) -> None:

        """Instantiates a SynapseStorage object

        Args:
            syn: synapse client; if not provided instantiate one
            token: if provided, use to instantiate a synapse client and login using the toke
            storageFileview: synapse ID of fileview containing administrative storage metadata; 
            TODO: move away from specific project setup and work with an interface that Synapse specifies (e.g. based on schemas)
        """
     
        # login using a token 
        if token:
            self.syn = synapseclient.Synapse()
            self.syn.login(sessionToken = token)
        elif syn: # if no token, assume a logged in synapse client has been provided
            self.syn = syn

        self.storageFileview = storageFileview

        # get data in administrative fileview for this pipeline 
        self.setStorageFileviewTable()

        # default manifest name
        self.manifest = "synapse_storage_manifest.csv"


    def setStorageFileviewTable(self) -> None:
        """ 
            Gets all data in an administrative fileview as a pandas dataframe and sets the SynapseStorage storageFileviewTable attribute
            Raises: TODO 
                ValueError: administrative fileview not found.
        """
        # query fileview for all administrative data
        self.storageFileviewTable = self.syn.tableQuery("SELECT * FROM " + self.storageFileview).asDataFrame()
   

    def getPaginatedRestResults(currentUserId : str) -> dict:
        """
            Gets the paginated results of the REST call to Synapse to check what projects the current user is part of.

            Args:
                currentUserId: synapse id for the user whose projects we want to get 
            
            Returns: a dictionary with a next page token and the results
        """
        all_results = syn.restGET('/projects/user/{principalId}'.format(principalId=currentUserId))
    
        while 'nextPageToken' in all_results: # iterate over next page token in results while there is any
            results_token = syn.restGET('/projects/user/{principalId}?nextPageToken={nextPageToken}'.format(principalId=currentUserId, nextPageToken = all_results['nextPageToken']))
            all_results['results'].extend(results_token['results'])

            if 'nextPageToken' in results_token:
                all_results['nextPageToken'] = results_token['nextPageToken']
            else:
                del(all_results['nextPageToken'])

        return all_results


    def getStorageProjects(self) -> list: 
    
        """ get all storage projects the current user has access to
        within the scope of the storage fileview parameter specified as SynapseStorage attribute

        Returns: a list of storage projects the current user has access to; the list consists of tuples (projectId, projectName) 
        """

        # get the set of all storage Synapse project accessible for this pipeline
        storageProjects = self.storageFileviewTable["projectId"].unique()

        # get the set of storage Synapse project accessible for this user

        # get current user ID
        currentUser = self.syn.getUserProfile()
        currentUserName = currentUser.userName 
        currentUserId = currentUser.ownerId
        
        # get a set of projects from Synapse 
        currentUserProjects = self.syn.restGET('/projects/user/{principalId}'.format(principalId=currentUserId))
        
        # prune results json filtering project id
        currentUserProjects = [currentUserProject["id"] for currentUserProject in currentUserProjects["results"]]

        # find set of user projects that are also in this pipeline's storage projects set
        storageProjects = list(set(storageProjects) & set(currentUserProjects))

        # prepare a return list of project IDs and names
        projects = []
        for projectId in storageProjects:
            projectName = self.syn.get(projectId, downloadFile = False).name
            projects.append((projectId, projectName))

        return projects


    def getStorageDatasetsInProject(self, projectId:str) -> list:
        
        """ get all datasets in folder under a given storage projects the current user has access to

        Args:
            projectId: synapse ID of a storage project
        Returns: a list of datasets within the given storage project; the list consists of tuples (datasetId, datasetName)
        Raises: TODO
            ValueError: Project ID not found.
        """
        
        # select all folders and their names w/in the storage project
        foldersTable = self.storageFileviewTable[(self.storageFileviewTable["type"] == "folder") & (self.storageFileviewTable["projectId"] == projectId)]

        # get an array of tuples (folderId, folderName)
        # some folders are part of datasets; others contain datasets
        # each dataset parent is the project; folders part of a dataset have another folder as a parent
        # to get folders if and only if they contain datasets for each folder 
        # check if folder's parent is the project; if so that folder contains a dataset 
        
        datasetList = [] 
        for folder in list(foldersTable[["id", "name"]].itertuples(index = False, name = None)):
            if self.syn.get(folder[0], downloadFile = False).properties["parentId"] == projectId:
                datasetList.append(folder)
        
        return datasetList


    def getFilesInStorageDataset(self, datasetId:str, fileNames:list = None) -> list:
        """ get all files in a given dataset folder 

        Args:
            datasetId: synapse ID of a storage dataset
            fileName: get a list of files with particular names; defaults to None in which case all dataset files are returned (except bookkeeping files, e.g.
            metadata manifests); if fileNames is not None all files matching the names in the fileNames list are returned if present
        Returns: a list of files; the list consist of tuples (fileId, fileName)
        Raises: TODO
            ValueError: Dataset ID not found.
        """

        # select all files within a given storage dataset (top level folder in a Synapse storage project)
        filesTable = self.storageFileviewTable[(self.storageFileviewTable["type"] == "file") & (self.storageFileviewTable["parentId"] == datasetId)]

        # return an array of tuples (fileId, fileName)
        fileList = []
        for row in filesTable[["id", "name"]].itertuples(index = False, name = None): 
            #if not row[1] == self.manifest and not fileNames:
            if not "manifest" in row[1] and not fileNames:
                # check if a metadata-manifest file has been passed in the list of filenames; assuming the manifest file has a specific filename, e.g. synapse_storage_manifest.csv; remove the manifest filename if so; (no need to add metadata to the metadata container); TODO: expose manifest filename as a configurable parameter and don't hard code.
                fileList.append(row)

            elif not fileNames == None and row[1] in fileNames:
                # if fileNames is specified and file is in fileNames add it to the returned list
                fileList.append(row)

        return fileList
        

    def getDatasetManifest(self, datasetId:str) -> str:
        """ get the manifest associated with a given dataset 

        Args:
            datasetId: synapse ID of a storage dataset
        Returns: a list of files; the list consist of tuples (fileId, fileName); returns empty list if no manifest is found
        """

        # get a list of files containing the manifest for this dataset (if any)
        
        manifest = self.getFilesInStorageDataset(datasetId, fileNames = [self.manifest])
        
        if not manifest:
            return []
        else:
            return manifest[0] # extract manifest tuple from list


    def getAllManifests(self) -> list:
        """ get all metadata manifest files across all datasets in projects a user has access to

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
        for (projectId, projectName) in projects:

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


    def associateMetadataWithFiles(self, metadataManifestPath:str, datasetId:str) -> str:
        """Associate metadata with files in a storage dataset already on Synapse. 
        Upload metadataManifest in the storage dataset folder on Synapse as well. Return synapseId of the uploaded manifest file.
        
            Args: metadataManifestPath path to csv containing a validated metadata manifest. The manifest should include a column entityId containing synapse IDs of files/entities to be associated with metadata, if that is applicable to the dataset type. Some datasets, e.g. clinical data, do not contain file id's, but data is stored in a table: one row per item. In this case, the system creates a file on Synapse for each row in the table (e.g. patient, biospecimen) and associates the columnset data as metadata/annotations to his file. 
            Returns: synapse Id of the uploaded manifest
            Raises: TODO
                FileNotFoundException: Manifest file does not exist at provided path.

        """

        # read manifest csv
        manifest = pd.read_csv(metadataManifestPath)


        # if there are no Synapse entities associated with the rows of this manifest
        # this may be due to data type (e.g. clinical data) being tabular 
        # and not requiring files; to utilize uniform interfaces downstream
        # (i.e. fileviews), a Synapse entity (a folder) is created for each row 
        # and an entity column is added to the manifest containing the resulting 
        # entity IDs; a table is also created at present as an additional interface 
        # for downstream query and interaction with the data. 
        # TODO: associate metadata with objects in the same loop iteration;
        # currently there is an extra iteration below.
        if not "entityId" in manifest.columns:
            entityIds = []
            
            for index, row in manifest.iterrows():
                rowEntity = Folder(datasetId + "_" + str(index), parent=datasetId)
                rowEntity = self.syn.store(rowEntity)
                entityIds.append(rowEntity["id"])

            manifest["entityId"] = entityIds
            # create and store a table corresponding to this dataset in this dataset parent project
            table = build_table(datasetId, self.syn.get(datasetId, downloadFile = False).properties["parentId"], manifest)
            table = self.syn.store(table)
            
            # update the manifest file, so that it contains the relevant entity IDs
            manifest.to_csv(metadataManifestPath, index = False)


        # use file ID (that is a synapse ID) as index of the dataframe
        manifest.set_index("entityId", inplace = True)

        # convert metadata in a form suitable for setting annotations on Synapse
        manifestMetadata = manifest.to_dict("index") 
        
        # get a schema explorer object to ensure schema attribute names used in manifest are translated to schema labels
        se = SchemaExplorer()

        # set annotations to files on Synapse
        for fileId, metadata in manifestMetadata.items():

            #  prepare metadata for Synapse storage (resolve display name into a name that Synapse annotations support (e.g no spaces)
            metadataSyn = {}
            for k, v in metadata.items():
                keySyn = se.get_class_label_from_display_name(str(k))
                if v:
                    valSyn = se.get_class_label_from_display_name(str(v))
                else:
                    valSyn = ""

                metadataSyn[keySyn] = valSyn

            self.syn.setAnnotations(fileId, metadataSyn)

        # store manifest to Synapse
        manifestSynapseFile = File(metadataManifestPath, description = "Manifest for dataset " + datasetId, parent = datasetId)

        manifestSynapseFileId = self.syn.store(manifestSynapseFile).id

        return manifestSynapseFileId
