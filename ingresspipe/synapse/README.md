## Usage of methods in `synapse.store` module

_Note: Refer to the `store_usage` module within the `examples/` directory here for the snippets._

**Make sure to configure the values of `"username"` and `"password"` in the `ingresspipe.config.config` package/module.**

To retreive a list of all the Synapse projects that the current user (user whose credentials are registered with the instance of `synapseclient.Synapse`), run the following:

```python
projects_list = syn_store.getStorageProjects()
logger.info("Testing retrieval of project list from Synapse...")
projects_df = pd.DataFrame(projects_list, columns=["Synapse ID", "Project Name"])
logger.info(projects_df)
```
The above snippet returns a dataframe with 'synapse ID' in the first column, and 'Project Name' in second.

From this list, select any project of your choice. Feed the synapse ID of the selected project to the `getStorageDatasetsInProject` method as follows:

_The below example uses the synapse ID of the `HTAN CenterA` project._

```python
datasets_list = syn_store.getStorageDatasetsInProject(projectId="syn20977135")
logger.info("Testing retrieval of dataset list within a given storage project from Synapse...")
datasets_df = pd.DataFrame(datasets_list, columns=["Synapse ID", "Dataset Name"])
logger.info(datasets_df)
```

Similarly, from the above list of datasets, select any dataset of your choice, and feed the synapse ID of that dataset to the `getFilesInStorageDataset` method as follows:

_The below example uses the synapse ID of "HTAN_CenterA_BulkRNAseq_AlignmentDataset_1" dataset._

```python
files_list = syn_store.getFilesInStorageDataset(datasetId="syn22125525")
logger.info("Testing retrieval of file list within a given storage dataset from Synapse")
files_df = pd.DataFrame(files_list, columns=["Synapse ID", "File Name"])
logger.info(files_df)
```

Once you have generated/filled out/validated a metadata manifest file, and want to associate it with a synapse dataset/entity, do the following:

```python
logger.info("Testing association of entities with annotation from manifest...")
manifest_syn_id = syn_store.associateMetadataWithFiles("./data/manifests/synapse_storage_manifest.csv", "syn21984120")
logger.info(manifest_syn_id)
```

_Note: Make sure you have the right permissions to the project before executing the above block of code._