# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text


class Dataset(object):

    """Dataset abstraction providing dataset attributes like dataset location, dataset location type,
    useful for storage functions. Can provide a dataset abstraction in the future
    """

    def __init__(self,
                 datasetLocation: str,
                 datasetLocationType: str) -> None:

        """Instantiates a Dataset object

        Args:
            datasetLocationType: local path, uri, synapse entity id; (e.g. gs://, syn123, /User/x/…); present location
            datasetLocationType: one of [local, gs, aws, synapse]; present location type
        """

        self.datasetLocation = datasetLocation
        self.datasetLocationType = datasetLocationType


    # setting mutators/accessors methods explicitly

    @property
    def datasetLocation(self) -> str:
        """Gets or sets the datasetLocation path"""
        return self.__datasetLocation

    @datasetLocation.setter
    def datasetLocation(self, datasetLocation) -> None:
        self.__datasetLocation = datasetLocation 
     
    @property
    def datasetLocationType(self) -> str:
        """Gets or sets the datasetLocationType"""
        return self.__datasetLocationType

    @datasetLocation.setter
    def datasetLocation(self, datasetLocationType) -> None:
        self.__datasetLocationType = datasetLocationType 

