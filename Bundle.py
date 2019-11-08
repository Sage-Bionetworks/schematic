import pandas as pd
import json

# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

from MetadataModel import MetadataModel
#from governance import Governance
from SynapseStorage import SynapseStorage

class Bundle(object):

    """

        - TODO: not currently part of the specification; to be defined.

    """

    def __init__(self,
                 dataset: Dataset,
                 mmodel: MetadataModel,
                 governance: Governance,
                 bundleUploadStorageType: str,
                 bundleStorage: Storage
                 ) -> None:

        """ Instantiates Bundle object

        Args: 
          dataset: a Dataset object 
          mmodel:  MetadataModel object
          governance: Governance object with permissions
          bundleUploadStorageType: one of [local, gs, aws, synapse]; present location type
          bundleStorage: Storage interface, upload/update bundle from Storage interface
        """


    # setting mutators/accessors methods explicitly

    # business logic

    def instantiateStorage(bundleUploadStorageType:str) -> None: 

        """ determine storage based on bundleUploadStorageType;
            setup storage specific parameters; check user auth
        Args:
          bundleUploadStorageType: storage type
        
        Returns: nothing, sets up storage specific parameters for self
        Raises: 
            ValueError: user not authorized
        """
        pass
