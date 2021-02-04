## Manifest Validation and Submission CLI Usage

The following command is a generic call to the programmatic interface for manifest validation and submission:

`$ poetry run schematic model --config /path/to/config.yml submit --manifest_path /path/to/manifest.csv --dataset_id <Synapse_Dataset_ID> --validate_component <Data_Model_Component>`

### Options Description

There are various optional arguments that can be passed to the `schematic model submit` interface.

`--config / -c`: Specify the path to the `config.yml` using this option. This is a required argument.

`--manifest_path / -mp`: Specify the path to the metadata manifest file that you want to submit to a dataset on `synapse.org`. This is a required argument.

`dataset_id / -d`: Specify the synID of the dataset folder on Synapse to which you intend to submit the metadata manifest file. This is a required argument.

`--validate_component / -vc`: The component or data type from the data model which you can use to validate the data filled in your manifest template.

### Run Example Command

```$ schematic model --config config.yml submit --manifest_path /path/to/patient_manifest.csv --dataset_id syn23643254 --validate_component Patient```
