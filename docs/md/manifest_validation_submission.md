## Manifest Validation and Submission CLI Usage

The following command is a generic call to the programmatic interface for manifest validation and submission:

`schematic model --config ~/path/to/config.yml submit --manifest_path ~/path/to/manifest.csv --dataset_id <Synapse_Dataset_ID> --validate_component <Data_Model_Component>`

### Run Example Command

```schematic model --config config.yml submit --manifest_path ~/path/to/patient_manifest.csv --dataset_id syn23643254 --validate_component Patient```
