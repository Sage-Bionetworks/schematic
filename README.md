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
                                                        examples/
                                                                 schemaExplorerUsage.py
                                         schemaGenerator/
                                                         __init__.py
                                                         schemaGeneratorModule.py
                                                         schemaGeneratorModuleReqs.py
                                                         examples/
                                                                  schemaGeneratorUsage.py
                                         manifestGenerator/
                                                           __init__.py
                                                           manifestGeneratorModule.py
                                                           manifestGeneratorModuleReqs.py
                                                           examples/
                                                                    manifestGeneratorUsage.py
                                         metadataModel/
                                                       __init__.py
                                                       metadataModelModule.py
                                                       metadataModelModuleReqs.py
                                                       examples/
                                                                metadataModelUsage.py
                                         synapseStore/
                                                     __init.py__
                                                     synapseStoreModule.py
                                                     synapseStoreModuleReqs.py
                                                     examples/
                                                              synapseStoreUsage.py
                                         utils/
                                               utils.py
                                         misc/
                                             misc.py
                       
                       tests/
                       setup/
                        
Note: The suffix "Reqs" indicates any other files that may be required by the main module.

# Contribution guidelines
* please consult CONTRIBUTION.md
