import synapseclient
from SynapseStorage import SynapseStorage

storage_fileview = "syn20446927"

syn = synapseclient.Synapse()
syn.login()


syn_store = SynapseStorage(storage_fileview, syn)



print("*****************************************************")
print("Testing retrieval of project list from Synapse")
print("*****************************************************")
projects_list = syn_store.getStorageProjects()

print(projects_list)


print("*****************************************************")
print("Testing retrieval of folder list within a given storage project from Synapse")
print("*****************************************************")
folder_list = syn_store.getStorageDatasetsInProject("syn19557917")

print(folder_list)


print("*****************************************************")
print("Testing retrieval of file list within a given storage dataseyt from Synapse")
print("*****************************************************")
file_list = syn_store.getFilesInStorageDataset("syn19557948")

print(file_list)


print("*****************************************************")
print("Testing association of antities with annotation from manifest")
print("*****************************************************")
manifest_syn_id = syn_store.associateMetadataWithFiles("./synapse_storage_manifest.csv", "syn20687304")

print(manifest_syn_id)
