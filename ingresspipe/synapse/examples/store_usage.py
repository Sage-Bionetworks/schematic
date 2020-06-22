import synapseclient
import logging
import pandas as pd

from ingresspipe.synapse.store import SynapseStorage
from ingresspipe.config.config import storage

logging.basicConfig(level=logging.INFO, format="%(message)s")

logger = logging.getLogger(__name__)

SYN_USERNAME = storage["Synapse"]["username"]
SYN_PASSWORD = storage["Synapse"]["password"]

# create an instance of synapseclient.Synapse() and login
syn = synapseclient.Synapse()

try:
    syn.login(SYN_USERNAME, SYN_PASSWORD)
except synapseclient.core.exceptions.SynapseNoCredentialsError:
    logger.error("Please make sure the 'username' and 'password' keys in config have been filled out.")
except synapseclient.core.exceptions.SynapseAuthenticationError:
    logger.error("Please make sure the credentials in the config file are correct.")

syn_store = SynapseStorage(syn=syn)

# testing the retrieval of list of projects (associated with current user) from synapse
projects_list = syn_store.getStorageProjects()
logger.info("Testing retrieval of project list from Synapse...")
# create pandas df from the list of projects to make results more presentable
projects_df = pd.DataFrame(projects_list, columns=["Synapse ID", "Project Name"])
logger.info(projects_df)

# testing the retrieval of list of datasets (associated with given project) from Synapse
# synapse ID for the "HTAN CenterA" project
datasets_list = syn_store.getStorageDatasetsInProject(projectId="syn20977135")
logger.info("Testing retrieval of dataset list within a given storage project from Synapse...")
datasets_df = pd.DataFrame(datasets_list, columns=["Synapse ID", "Dataset Name"])
logger.info(datasets_df)

# testing the retrieval of list of files (associated with given dataset) from Synapse
# synapse ID of the "HTAN_CenterA_BulkRNAseq_AlignmentDataset_1" dataset
files_list = syn_store.getFilesInStorageDataset(datasetId="syn22125525")
logger.info("Testing retrieval of file list within a given storage dataset from Synapse")
files_df = pd.DataFrame(files_list, columns=["Synapse ID", "File Name"])
logger.info(files_df)

# testing the association of entities with annotation(s) from manifest
# synapse ID of "HTAN_CenterA_FamilyHistory" dataset and associating with it a validated manifest
logger.info("Testing association of entities with annotation from manifest...")
manifest_syn_id = syn_store.associateMetadataWithFiles("./data/manifests/synapse_storage_manifest.csv", "syn21984120")
logger.info(manifest_syn_id)

# testing the successful retreival of all manifests associated with a project, accessible by the current user
logger.info("Testing retreival of all manifests associated with projects accessible by user...")
manifests_list = syn_store.getAllManifests()
manifests_df = pd.DataFrame(manifests_list)
logger.info(manifests_df)