import synapseclient
from SynapseStorage import SynapseStorage

storage_fileview = "syn20446927"

# test with a synapse client object login 
syn = synapseclient.Synapse()
syn.login()
syn_store = SynapseStorage(storage_fileview, syn = syn)

# test with a synapse login token (e.g. can test locally by capturing a browser cookie after login to Synapse) 
# syn_store = SynapseStorage(storage_fileview, token = "token_string")


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
print("Testing retrieval of file list within a given storage dataset from Synapse")
print("*****************************************************")
file_list = syn_store.getFilesInStorageDataset("syn19557948")

print(file_list)


print("*****************************************************")
print("Testing association of entities with annotation from manifest")
print("*****************************************************")
manifest_syn_id = syn_store.associateMetadataWithFiles("./synapse_storage_manifest.csv", "syn20687304")

print(manifest_syn_id)
