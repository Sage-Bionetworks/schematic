## Manifest Validation and Submission CLI Usage

The following command is a generic call to the programmatic interface for manifest validation and submission:

`schematic model --config ~/path/to/config.yml submit --manifest_path ~/path/to/manifest.csv --dataset_id <Synapse_Dataset_ID> --validate_component <Data_Model_Component>`

### Run Example Command

```schematic model --config config.yml submit --manifest_path ~/path/to/patient_manifest.csv --dataset_id syn23643254 --validate_component Patient```

### Hybrid Validation Rule Distribution
As of March 29, 2022, the hybrid validation rules are as follows:


    In-house Validation Rules:
    num
    int/float/string
    list::regex
    regex module
    list
    url
    matchAtLeastOne
    matchExactlyOne

    Great Expectations:
    num
    int/float/string
    recommended
    protectAges
    unique
    inRange

