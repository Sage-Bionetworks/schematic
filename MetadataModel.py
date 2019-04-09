import pandas as pd
import networkx as nx

# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

# handle schema logic; to be refactored as SchemaExplorer matures into a package
# as collaboration with Biothings progresses
from schema_explorer import SchemaExplorer

class MetadataModel(object):

    """Metadata model wrapper around schema.org specification graph.
    Provides basic utilities to 

    1) manipulate the metadata model;
    2) generate metadata model views:
        - generate manifest view of the metadata metadata model
        - usage getModelManifest(rootNode)

        - generate validation schemas view of the metadata model;
        - TODO: not currently part of the specification; to be defined.

    """

    def __init__(self,
                 SchemaExplorer: se,
                 inputMModelLocation: str,
                 inputMModelLocationType: str,
                 ) -> None:

        """ Instantiates MetadataModel object

        Args: 
          se: a SchemaExplorer instance 
          inputMModelLocation:  local path, uri, synapse entity id; (e.g. gs://, syn123, /User/x/…); present location
          inputMModelLocationType: one of [local, gs, aws, synapse]; present location type
        """


    # setting mutators/accessors methods explicitly

    @property
     def inputMModelLocation(self) -> str:
         """Gets or sets the inputModelLocation path"""
         return self._getInputModelLocation()

     @inputMModelLocation.setter
     def inputMModelLocation(self, inputMModelLocation) -> None:
         return self._setInputModelLocation(inputModelLocation)

     def _getInputModelLocation(self) -> str:
         """Indirect accessor to get inputModelLocation property."""
         return self.inputMModelLocation

     def _setInputModelLocation(self, inputModelLocation) -> None:
         """Indirect setter to set the 'inputModelLocation' property."""
         self.inputMModelLocation = inputModelLocation

     @property
     def inputMModelLocationType(self) -> str:
         """Gets or sets the inputModelLocationType"""
         return self._getInputModelLocationType()

     @inputMModelLocationType.setter
     def inputMModelLocationType(self, inputMModelLocationType) -> None:
         return self._setInputModelLocationType(inputModelLocationType)

     def _getInputModelLocationType(self) -> str:
         """Indirect accessor to get inputModelLocationType property."""
         return self.inputMModelLocationType

     def _setInputModelLocationType(self, inputModelLocationType: str) -> None:
         """Indirect setter to set the 'inputModelLocationType' property."""
         self.inputMModelLocationType = inputModelLocationType

     @property
     def se(self) -> SchemaExplorer:
         """Gets or sets the SchemaExplorer instance"""
         return self._getSchemaExplorer()

     @se.setter
     def se(self, se: SchemaExplorer) -> None:
         return self._setSchemaExplorer(se)

     def _getSchemaExplorer(self) -> SchemaExplorer:
         """Indirect accessor to get SchemaExplorer property."""
         return self.se

     def _setSchemaExplorer(self, se: SchemaExplorer) -> None:
         """Indirect setter to set the 'SchemaExplorer property."""
         self.se = se 


    # business logic

    def loadMModel(self) -> None:
        """ load Schema; handles schema file input and sets mmodel
        """
        pass


    def getModelSubgraph(rootNode: str, 
                         subgraphType: str) -> nx.DiGraph:
        """ get a schema subgraph from rootNode descendants on edges/node properties of type subgraphType
        Args:
          rootNode: a schema node label (i.e. term)
          subgraphType: the kind of subgraph to traverse (i.e. based on node properties or edge labels)
        
        Returns: a directed graph (networkx DiGraph) subgraph of the metadata model w/ vertex set root node descendants

        Raises: 
            ValueError: rootNode not found in metadata model.
        """
        pass


    def getModelManifest(rootNode:String) -> pd.DataFrame: 

        """ get annotations manifest dataframe 
        Args:
          rootNode: a schema node label (i.e. term)
        
        Returns: a pandas data frame 
        Raises: 
            ValueError: rootNode not found in metadata model.
        """
        pass
