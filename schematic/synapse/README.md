## Usage of methods in `synapse.store` module

_Note: Refer to the `store_usage` module within the `examples/` directory here for the snippets._

**Make sure to configure the values of `"username"` and `"password"` in the `.synapseConfig` file as described in the main README.**

To retreive a list of all the Synapse projects that the current user (user whose credentials are registered with the instance of `synapseclient.Synapse`), run the following:

```python
projects_list = syn_store.getStorageProjects()
print("Testing retrieval of project list from Synapse...")
projects_df = pd.DataFrame(projects_list, columns=["Synapse ID", "Project Name"])
print(projects_df)
```
The above snippet returns a dataframe with 'synapse ID' in the first column, and 'Project Name' in second.

From this list, select any project of your choice. Feed the synapse ID of the selected project to the `getStorageDatasetsInProject()` method as follows:

_The below example uses the synapse ID of the `HTAN CenterA` project._

```python
datasets_list = syn_store.getStorageDatasetsInProject(projectId="syn20977135")
print("Testing retrieval of dataset list within a given storage project from Synapse...")
datasets_df = pd.DataFrame(datasets_list, columns=["Synapse ID", "Dataset Name"])
print(datasets_df)
```

Similarly, from the above list of datasets, select any dataset of your choice, and feed the synapse ID of that dataset to the `getFilesInStorageDataset()` method as follows:

_The below example uses the synapse ID of "HTAN_CenterA_BulkRNAseq_AlignmentDataset_1" dataset._

```python
files_list = syn_store.getFilesInStorageDataset(datasetId="syn22125525")
print("Testing retrieval of file list within a given storage dataset from Synapse")
files_df = pd.DataFrame(files_list, columns=["Synapse ID", "File Name"])
print(files_df)
```

Once you have generated/filled out/validated a metadata manifest file, and want to associate it with a synapse dataset/entity, do the following:

```python
print("Testing association of entities with annotation from manifest...")
manifest_syn_id = syn_store.associateMetadataWithFiles(os.path.join('DATA_PATH', config_data["synapse"]["manifest_filename"]), "syn21984120")
print(manifest_syn_id)
```

_Note: Make sure you have the right permissions to the project before executing the above block of code._