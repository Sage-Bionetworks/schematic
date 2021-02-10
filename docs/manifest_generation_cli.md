## Manifest Generation CLI Usage

The following command is a generic call to the programmatic interface for manifest generation:

```$ schematic manifest --config /path/to/config.yml get --title <Manifest_Title> --data_type <Data_Model_Component> --jsonld /path/to/data_model.jsonld --dataset_id <Synapse_Dataset_ID> --sheet_url <True_or_False> --json_schema /path/to/json_validation_schema.json```

### Options Description

There are various optional arguments that can be passed to the `schematic manifest get` interface.

`--config / -c`: Specify the path to the `config.yml` using this option. This is a required argument.

`--title / -t`: Specify the title of the manifest that will be created at the end of the run. You can either explicitly pass the title of the manifest here or provide it in the `config.yml` file as a value for the `(manifest > title)` key.

`--data_type / -dt`: Specify the component (data type) from the data model that is to be used for generating the metadata manifest file. You can either explicitly pass the data type here or provide it in the `config.yml` file as a value for the `(manifest > data_type)` key.

`--dataset_id / -d`: Specify the synID of a dataset folder on [`synapse.org`](https://www.synapse.org/). If there is an exisiting manifest already present in that folder, then it will be pulled with the existing annotations for further annotation/modification. 

`--sheet_url / -s`: Takes `True` or `False` as argument values. If `True` then it will produce a Googlesheets URL/link to the metadata manifest file. If `False`, then it will produce a pandas dataframe for the same.

`--json_schema / -j`: Specify the path to the JSON Validation Schema for this argument. You can either explicitly pass the `.json` file here or provide it in the `config.yml` file as a value for the `(model > input > validation_schema)` key.

### Run Example Command

```$ schematic manifest get --config /path/to/config.yml```
