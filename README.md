# HTAN Data Ingress Pipeline

## Directory Structure

A bird's eye view of structure and classification of various modules / packages within this repository:
      
      HTAN-data-pipeline/
                        data/
                        docs/
                        ingresspipe/
                                    manifest/
                                                # package & modules for handling manifests
                                                __init__.py
                                                generator.py
                                                examples/
                                                            schema_explorer_usage.py
                                    models/
                                                # package & modules for wrapping data models
                                                __init__.py
                                                metadata.py
                                                examples/
                                                            schema_generator_usage.py
                                    schemas/
                                                # packages & modules for handling schemas
                                                __init__.py
                                                explorer.py
                                                generator.py
                                                examples/
                                                            explorer_usage.py
                                                            generator_usage.py
                                    synapse/
                                                # packages and modules for interacting with Synapse
                                                __init.py__
                                                store.py
                                                examples/
                                                            synapse_store_usage.py
                                    utils/
                                                utils.py
                       
                        tests/
                        setup/

See below a recursive directory listing, which summarizes the file(s) / folder(s) listing that you see above:
```bash
.
├── CONTRIBUTION.md
├── README.md
├── data
├── docs
├── ingresspipe
│   ├── manifest
│   │   └── __init__.py
│   ├── models
│   │   └── __init__.py
│   ├── schemas
│   │   ├── __init__.py
│   │   ├── examples
│   │   │   └── generator_usage.py
│   │   └── generator.py
│   ├── synapse
│   │   └── __init__.py
│   └── utils
│       └── utils.py
├── setup
└── tests
```
# Contribution guidelines

Clone a copy of the repository here:
      
      git clone --single-branch --branch organized-into-packages https://github.com/sujaypatil96/HTAN-data-pipeline.git

Modify your files, add them to the staging area, use a descriptive commit message and push to the same branch as a pull request for review.

* Please consult CONTRIBUTION.md for further reference
