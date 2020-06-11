# HTAN Data Ingress Pipeline

## Directory Structure

A bird's eye view of structure and classification of various modules / packages within this repository:
      
      HTAN-data-pipeline/
                        data/
                        docs/
                        ingresspipe/
                                    __init__.py
                                    manifest/
                                                # package & modules for handling manifests
                                                __init__.py
                                                generator.py
                                                examples/
                                                            manifest_explorer_usage.py
                                    models/
                                                # package & modules for wrapping data models
                                                __init__.py
                                                metadata.py
                                                examples/
                                                            metadata_model_usage.py
                                    schemas/
                                                # packages & modules for handling schemas
                                                __init__.py
                                                explorer.py
                                                generator.py
                                                examples/
                                                            schema_explorer_usage.py
                                                            schema_generator_usage.py
                                    synapse/
                                                # packages and modules for interacting with Synapse
                                                __init.py__
                                                store.py
                                                examples/
                                                            synapse_store_usage.py
                                    utils/
                                                __init__.py
                                                utils.py
                                    config/
                                                __init__.py
                                                config.py
                       
                        tests/
                        setup/

See below a recursive directory listing, which summarizes the file(s) / folder(s) listing that you see above:
```bash
.
├── CONTRIBUTION.md
├── README.md
├── data
│   └── HTAN.jsonld
├── docs
│   └── doc.txt
├── ingresspipe
│   ├── __init__.py
│   ├── config
│   │   ├── __init__.py
│   │   └── config.py
│   ├── manifest
│   │   ├── __init__.py
│   │   └── generator.py
│   ├── models
│   │   ├── __init__.py
│   │   └── metadata.py
│   ├── schemas
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── curie.py
│   │   ├── data
│   │   │   ├── all_layer.jsonld
│   │   │   ├── biothings.jsonld
│   │   │   ├── class_json_schema.json
│   │   │   ├── new_schema.jsonld
│   │   │   ├── property_json_schema.json
│   │   │   └── schema.json
│   │   ├── explorer.py
│   │   ├── generator.py
│   │   ├── tests
│   │   │   ├── data
│   │   │   │   ├── biothings_duplicate_test.jsonld
│   │   │   │   ├── biothings_test.jsonld
│   │   │   │   ├── property_schema_missing_domain.json
│   │   │   │   └── property_schema_missing_range.json
│   │   │   └── test_schema_validator.py
│   │   └── utils.py
│   └── utils
│       ├── __init__.py
│       └── general.py
├── requirements.txt
├── setup
│   └── setup.txt
└── tests
    └── test.py
```
## Contribution Guidelines

Clone a copy of the repository here:
      
      git clone --single-branch --branch organized-into-packages https://github.com/sujaypatil96/HTAN-data-pipeline.git

Modify your files, add them to the staging area, use a descriptive commit message and push to the same branch as a pull request for review.
