import pprint
import uuid
import json
import os
import time

import synapseclient
from synapseclient import File

from SynapseStorage import SynapseStorage


pp = pprint.PrettyPrinter(indent = 3)

storage_fileview = "syn20446927"

# test with a synapse client object login 
syn = synapseclient.Synapse()
syn.login()

syn_store = SynapseStorage(syn = syn)
"""
# test with a synapse login token (e.g. can test locally by capturing a browser cookie after login to Synapse) 
# syn_store = SynapseStorage(storage_fileview, token = "token_string")

print("*****************************************************")
print("Testing retrieval of project list from Synapse")
print("*****************************************************")
projects_list = syn_store.getStorageProjects()

print(projects_list)


print("*****************************************************")
print("Testing retrieval of dataset list within a given storage project from Synapse")
print("*****************************************************")
folder_list = syn_store.getStorageDatasetsInProject("syn20687304")

print(folder_list)


print("*****************************************************")
print("Testing retrieval of file list within a given storage dataset from Synapse")
print("*****************************************************")
file_list = syn_store.getFilesInStorageDataset("syn19557948")

print(file_list)
"""

print("*****************************************************")
print("Testing association of entities with annotation from manifest")
print("*****************************************************")
manifest_syn_id = syn_store.associateMetadataWithFiles("./synapse_storage_manifest.csv", "syn21893757")

print(manifest_syn_id)

print("*****************************************************")
print("Testing getting all manifests associated with a project accessible by user")
print("*****************************************************")
manifests = syn_store.getAllManifests()
pp.pprint(manifests)


print("*****************************************************")
print("Testing updating fileset in a manifest associated with a dataset")
print("*****************************************************")

# dataset whose manifest is updated
dataset_id = "syn21893757"

# might want to add files to the dataset to see if they are reflected in the manifest; please allow some time for Synapse to index the files

manifestId = syn_store.update_dataset_manifest_files(dataset_id)
pp.pprint(manifestId)

