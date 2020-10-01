import synapseclient
import pandas as pd
import os

from schematic.synapse.store import SynapseStorage

from schematic.utils.config_utils import load_yaml

from definitions import ROOT_DIR, CONFIG_PATH, DATA_PATH

config_data = load_yaml(CONFIG_PATH)

PATH_TO_SYN_CONF = os.path.join(ROOT_DIR, '.synapseConfig')

# create an instance of synapseclient.Synapse() and login
syn = synapseclient.Synapse(configPath=PATH_TO_SYN_CONF)

try:
    syn.login()
except synapseclient.core.exceptions.SynapseNoCredentialsError:
    print("Please make sure the 'username' and 'password'/'api_key' values have been filled out in .synapseConfig.")
except synapseclient.core.exceptions.SynapseAuthenticationError:
    print("Please make sure the credentials in the .synapseConfig file are correct.")

syn_store = SynapseStorage(syn=syn)

# testing the retrieval of list of projects (associated with current user) from synapse
projects_list = syn_store.getStorageProjects()
print("Testing retrieval of project list from Synapse...")
# create pandas df from the list of projects to make results more presentable
projects_df = pd.DataFrame(projects_list, columns=["Synapse ID", "Project Name"])
print(projects_df)

# testing the retrieval of list of datasets (associated with given project) from Synapse
# synapse ID for the "HTAN CenterA" project
datasets_list = syn_store.getStorageDatasetsInProject(projectId="syn20977135")
print("Testing retrieval of dataset list within a given storage project from Synapse...")
datasets_df = pd.DataFrame(datasets_list, columns=["Synapse ID", "Dataset Name"])
print(datasets_df)

# testing the retrieval of list of files (associated with given dataset) from Synapse
# synapse ID of the "HTAN_CenterA_BulkRNAseq_AlignmentDataset_1" dataset
files_list = syn_store.getFilesInStorageDataset(datasetId="syn22125525")
print("Testing retrieval of file list within a given storage dataset from Synapse")
files_df = pd.DataFrame(files_list, columns=["Synapse ID", "File Name"])
print(files_df)

# testing the association of entities with annotation(s) from manifest
# synapse ID of "HTAN_CenterA_FamilyHistory" dataset and associating with it a validated manifest
MANIFEST_LOC = os.path.join(DATA_PATH, '', config_data["synapse"]["manifest_filename"])
print("Testing association of entities with annotation from manifest...")
manifest_syn_id = syn_store.associateMetadataWithFiles(MANIFEST_LOC, "syn21984120")
print(manifest_syn_id)

# testing the successful retreival of all manifests associated with a project, accessible by the current user
print("Testing retreival of all manifests associated with projects accessible by user...")
manifests_list = syn_store.getAllManifests()
manifests_df = pd.DataFrame(manifests_list)
print(manifests_df)