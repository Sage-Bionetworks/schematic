# HTAN Data Ingress Pipeline

## Directory Structure

A bird's eye view of structure and classification of various modules / packages within this repository:
    
    HTANdataPipeline/
                    data/
                    docs/
                    dataIngressPipeline/
                                        schemaExplorer/
                                                      __init__.py
                                                      schemaExplorerModule.py
                                                      schemaExplorerModuleReqs.py
                                                      tests
                                        manifest/
                                                __init__.py
                                                manifestGeneratorModule.py
                                                manifestGeneratorModuleReqs.py
                                        metadataModel/
                                                      __init__.py
                                                      metadataModelModule.py
                                                      metadataModelModuleReqs.py
                                        synapseStore/
                                                    __init.py__
                                                    synapseStoreModule.py
                                                    synapseStoreModuleReqs.py
                                        utils/
                                              utils.py
                                        misc/
                                            misc.py
                       
                      tests/
                      setup/
                        
Note: The suffix "Reqs" indicates any other files that may be required by the main module.

# Contribution guidelines
* please consult CONTRIBUTION.md
