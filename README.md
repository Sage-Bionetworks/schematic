# HTAN Data Ingress Pipeline

## Directory Structure

A bird's eye view of structure and classification of various modules / packages within this repository:
    
    HTAN-data-pipeline/
                       data/
                       docs/
                       data-ingress-pipeline/
                                             schema-explorer/
                                                             __init__.py
                                                             schema_explorer_module.py
                                                             schema_explorer_module_reqs.py
                                                             examples/
                                                                      schema_explorer_usage.py
                                             schema-generator/
                                                              __init__.py
                                                              schema_generator_module.py
                                                              schema_generator_module_reqs.py
                                                              examples/
                                                                       schema_generator_usage.py
                                             manifest-generator/
                                                                __init__.py
                                                                manifest_generator_module.py
                                                                manifest_generator_module_reqs.py
                                                                examples/
                                                                         manifest_generator_usage.py
                                             metadata-model/
                                                            __init__.py
                                                            metadata_model_module.py
                                                            metadata_model_module_reqs.py
                                                            examples/
                                                                     metadata_model_usage.py
                                             synapse-store/
                                                           __init.py__
                                                           synapse_store_module.py
                                                           synapse_store_module_reqs.py
                                                           examples/
                                                                    synapse_store_usage.py
                                         utils/
                                               utils.py
                                         misc/
                                             misc.py
                       
                       tests/
                       setup/
                        
Note: The suffix "reqs" indicates any other files that may be required by the main module.

# Contribution guidelines
* please consult CONTRIBUTION.md
