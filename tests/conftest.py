from multiprocessing.sharedctypes import Value
import os
import logging
import sys

import shutil
import pytest
import pandas as pd
from dotenv import load_dotenv, find_dotenv

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.configuration.configuration import CONFIG
from schematic.utils.df_utils import load_df

load_dotenv()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Silence some very verbose loggers
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("googleapiclient").setLevel(logging.INFO)
logging.getLogger("google_auth_httplib2").setLevel(logging.INFO)


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")

@pytest.fixture(scope="session")
def dataset_id():
    yield "syn25614635"


# This class serves as a container for helper functions that can be
# passed to individual tests using the `helpers` fixture. This approach
# was required because fixture functions cannot take arguments.
class Helpers:
    @staticmethod
    def get_data_path(path, *paths):
        return os.path.join(DATA_DIR, path, *paths)

    @staticmethod
    def get_data_file(path, *paths, **kwargs):
        fullpath = os.path.join(DATA_DIR, path, *paths)
        return open(fullpath, **kwargs)

    @staticmethod
    def get_data_frame(path, *paths, **kwargs):
        fullpath = os.path.join(DATA_DIR, path, *paths)
        return load_df(fullpath, **kwargs)

    
    @staticmethod
    def get_data_model_explorer(path=None, *paths):
        #commenting this now bc we dont want to have multiple instances
        if path is None:
            return

        fullpath = Helpers.get_data_path(path, *paths)

        # Instantiate DataModelParser
        data_model_parser = DataModelParser(path_to_data_model = fullpath)
        
        #Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(parsed_data_model)

        # Generate graph
        graph_data_model = data_model_grapher.generate_data_model_graph()

        #Instantiate DataModelGraphExplorer
        DME = DataModelGraphExplorer(graph_data_model)

        return DME

    @staticmethod
    def get_data_model_parser(data_model_name:str=None, *paths):
        # Get path to data model
        fullpath = Helpers.get_data_path(path=data_model_name, *paths)
        # Instantiate DataModelParser
        data_model_parser = DataModelParser(path_to_data_model=fullpath)
        return data_model_parser
    
    

    @staticmethod
    def get_python_version(self):
        version=sys.version
        base_version=".".join(version.split('.')[0:2])

        return base_version

    @staticmethod
    def get_python_project(self):

        version = self.get_python_version(Helpers)

        python_projects = {
            "3.7":  "syn47217926",
            "3.8":  "syn47217967",
            "3.9":  "syn47218127",
            "3.10": "syn47218347",
        }

        return python_projects[version]

@pytest.fixture(scope="session")
def helpers():
    yield Helpers

@pytest.fixture(scope="session")
def config():
    yield CONFIG
