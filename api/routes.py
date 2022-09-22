import os
import shutil
import tempfile
import shutil
from urllib.parse import non_hierarchical
import urllib.request

import connexion
from connexion.decorators.uri_parsing import Swagger2URIParser
from flask import current_app as app, request, g, jsonify
from werkzeug.debug import DebuggedApplication

from schematic import CONFIG

from schematic.manifest.generator import ManifestGenerator
from schematic.models.metadata import MetadataModel
from schematic.schemas.generator import SchemaGenerator
from schematic.schemas.explorer import SchemaExplorer
from schematic.store.synapse import SynapseStorage
from schematic.schemas.explorer import SchemaExplorer
import pandas as pd
import json
from schematic.utils.df_utils import load_df
import pickle

# def before_request(var1, var2):
#     # Do stuff before your route executes
#     pass
# def after_request(var1, var2):
#     # Do stuff after your route executes
#     pass


def config_handler(asset_view=None):
    path_to_config = app.config["SCHEMATIC_CONFIG"]

    # check if file exists at the path created, i.e., app.config['SCHEMATIC_CONFIG']
    if os.path.isfile(path_to_config):
        CONFIG.load_config(path_to_config, asset_view = asset_view)

    else:
        raise FileNotFoundError(
            f"No configuration file was found at this path: {path_to_config}"
        )

class JsonConverter:
    '''
    Mainly handle converting json str or json file to csv
    '''
    def readJson(self, json_str=None, manifest_file=None):
        '''
        The purpose of this function is to read either json str or json file
        input: 
            json_str: json object
            manifest_file: manifest file object 
        output: 
            return a dataframe
        '''
        if json_str:
            df = pd.read_json(json_str)
        elif manifest_file: 
            df = pd.read_json(manifest_file.read())
        return df
    
    def get_file(self, file_key):
        '''
        The purpose of this function is to get the file uploaded by user
        input: 
            file_key: Defined in api.yaml. This key refers to the files uploaded. 
            manifest_file: manifest file object 
        output: 
            return file object
        '''

        manifest_file = connexion.request.files[file_key]
        return manifest_file

    def IsJsonFile(self, manifest_file):
        '''
        The purpose of this function is check if the manifest file that gets uploaded is a json or not
        input: 
            manifest_file: manifest file object 
        output: 
            return True if it is json
        '''

        file_type = manifest_file.content_type
        if file_type == 'application/json':
            return True
        else: 
            return False

    def convert_df_to_csv(self, df, file_name):
        '''
        The purpose of this function is to convert dataframe to a temporary CSV file
        input: 
            df: dataframe
            file_name: file name of the output csv
        output: 
            return temporary file path of the output csv
        '''

        # convert dataframe to a temporary csv file
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, file_name)
        df.to_csv(temp_path, encoding = 'utf-8', index=False)
        return temp_path

    def convert_json_str_to_csv(self, json_str, file_name):
        '''
        The purpose of this function is to convert json str to a temporary csv file
        input: 
            json_str: json object
            file_name: file name of the output csv
        output: 
            return temporary file path of the output csv
        '''

        # convert json to df
        df = self.readJson(json_str = json_str)

        # convert dataframe to a temporary csv file
        temp_path = self.convert_df_to_csv(df, file_name)

        return temp_path

    def convert_json_file_to_csv(self, file_key):
        '''
        The purpose of this function is to convert json str to a temporary csv file
        input: 
            file_key: Defined in api.yaml. This key refers to the files uploaded. 
        output: 
            return temporary file path of the output csv
        '''

        # get manifest file
        manifest_file = self.get_file(file_key)

        if self.IsJsonFile(manifest_file):
            # read json as dataframe
            df = self.readJson(manifest_file = manifest_file)
            # get base file name
            base = os.path.splitext(manifest_file.filename)[0]
            # name the new csv file 
            new_file_name = base + '.csv'
            # convert to csv
            temp_path = self.convert_df_to_csv(df, new_file_name)
            return temp_path
        else: 
            temp_path = save_file(file_key='file_name')
            return temp_path
        

        
def save_file(file_key="csv_file"):
    '''
    input: 
        file_key: Defined in api.yaml. This key refers to the files uploaded. By default, set to "csv_file"
    Return a temporary file path for the uploaded a given file
    '''
    manifest_file = connexion.request.files[file_key]

    # save contents of incoming manifest CSV file to temp file
    temp_dir = tempfile.gettempdir()
    # path to temp file where manifest file contents will be saved
    temp_path = os.path.join(temp_dir, manifest_file.filename)
    # save content
    manifest_file.save(temp_path)

    return temp_path

def initalize_metadata_model(schema_url):
    jsonld = get_temp_jsonld(schema_url)
    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType="local"
    )
    return metadata_model

def get_temp_jsonld(schema_url):
    # retrieve a JSON-LD via URL and store it in a temporary location
    with urllib.request.urlopen(schema_url) as response:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonld") as tmp_file:
            shutil.copyfileobj(response, tmp_file)

    # get path to temporary JSON-LD file
    return tmp_file.name

# @before_request
def get_manifest_route(schema_url, title, oauth, use_annotations, dataset_ids=None, asset_view = None):
    # call config_handler()
    config_handler(asset_view = asset_view)

    # get path to temporary JSON-LD file
    jsonld = get_temp_jsonld(schema_url)

    # Gather all data_types to make manifests for.
    all_args = connexion.request.args
    args_dict = dict(all_args.lists())
    data_type = args_dict['data_type']
    
    # Gather all dataset_ids
    try:
        dataset_ids = args_dict['dataset_id']
    except:
        pass
    
    if dataset_ids:
        # Check that the number of submitted data_types matches
        # the number of dataset_ids (if applicable)
        len_data_types = len(data_type)
        len_dataset_ids = len(dataset_ids)
        
        try:
            len_data_types == len_dataset_ids
        except:
            raise ValueError(
                    f"There is a mismatch in the number of data_types and dataset_id's that "
                    f"submitted. Please check your submission and try again."
                )
        
        # Raise an error if used in conjunction with datatype = 'all_manifests'
        try:
            data_type[0] != 'all manifests'
        except:
            raise ValueError(
                    f"When submitting 'all manifests' as the data_type cannot also submit dataset_id. "
                    f"Please check your submission and try again."
                )


    def create_single_manifest(data_type, dataset_id=None):
        # create object of type ManifestGenerator
        manifest_generator = ManifestGenerator(
            path_to_json_ld=jsonld,
            title=t,
            root=data_type,
            oauth=oauth,
            use_annotations=use_annotations,
            alphabetize_valid_values = 'ascending',
        )

        result = manifest_generator.get_manifest(
            dataset_id=dataset_id, sheet_url=True,
        )
               
        return result

    # Gather all returned result urls
    all_results = []
    if data_type[0] == 'all manifests':
        sg = SchemaGenerator(path_to_json_ld=jsonld)
        component_digraph = sg.se.get_digraph_by_edge_type('requiresComponent')
        components = component_digraph.nodes()
        for component in components:
            t = f'{title}.{component}.manifest'
            result = create_single_manifest(data_type = component)
            all_results.append(result)
    else:
        for i, dt in enumerate(data_type):
            if len(data_type) > 1:
                t = f'{title}.{dt}.manifest'
            else:
                t = title

            if dataset_ids:
                # if a dataset_id is provided add this to the function call.
                result = create_single_manifest(data_type = dt, dataset_id = dataset_ids[i])
            else:
                result = create_single_manifest(data_type = dt)
            all_results.append(result)

    return all_results


def validate_manifest_route(schema_url, data_type):
    # call config_handler()
    config_handler()

    #Get path to temp file where manifest file contents will be saved
    temp_path = save_file()

    # get path to temporary JSON-LD file
    jsonld = get_temp_jsonld(schema_url)

    metadata_model = MetadataModel(
        inputMModelLocation=jsonld, inputMModelLocationType="local"
    )

    errors, warnings = metadata_model.validateModelManifest(
        manifestPath=temp_path, rootNode=data_type
    )
    
    res_dict = {"errors": errors, "warnings": warnings}

    return res_dict


def submit_manifest_route(schema_url, asset_view=None, manifest_record_type=None, json_str=None):
    # call config_handler()
    config_handler(asset_view = asset_view)

    # convert Json file to CSV if applicable
    jsc = JsonConverter()
    if json_str:
        temp_path = jsc.convert_json_str_to_csv(json_str = json_str, file_name = "example_json")
    else: 
        temp_path = jsc.convert_json_file_to_csv("file_name")


    dataset_id = connexion.request.args["dataset_id"]

    data_type = connexion.request.args["data_type"]

    restrict_rules = connexion.request.args["restrict_rules"]

    metadata_model = initalize_metadata_model(schema_url)

    input_token = connexion.request.args["input_token"]

    if data_type == 'None':
        validate_component = None
    else:
        validate_component = data_type

    manifest_id = metadata_model.submit_metadata_manifest(
        path_to_json_ld = schema_url, manifest_path=temp_path, dataset_id=dataset_id, validate_component=validate_component, input_token=input_token, manifest_record_type = manifest_record_type, restrict_rules = restrict_rules)

    return manifest_id

def populate_manifest_route(schema_url, title=None, data_type=None):
    # call config_handler()
    config_handler()

    # get path to temporary JSON-LD file
    jsonld = get_temp_jsonld(schema_url)

    # Get path to temp file where manifest file contents will be saved
    temp_path = save_file()
   
    #Initalize MetadataModel
    metadata_model = MetadataModel(inputMModelLocation=jsonld, inputMModelLocationType='local')

    #Call populateModelManifest class
    populated_manifest_link = metadata_model.populateModelManifest(title=title, manifestPath=temp_path, rootNode=data_type)

    return populated_manifest_link


def get_storage_projects(input_token, asset_view):
    # call config handler 
    config_handler(asset_view=asset_view)

    # use Synapse storage 
    store = SynapseStorage(input_token=input_token)

    # call getStorageProjects function
    lst_storage_projects = store.getStorageProjects()
    
    return lst_storage_projects

def get_storage_projects_datasets(input_token, asset_view, project_id):
    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(input_token=input_token)

    # call getStorageDatasetsInProject function
    sorted_dataset_lst = store.getStorageDatasetsInProject(projectId = project_id)
    
    return sorted_dataset_lst


def get_files_storage_dataset(input_token, asset_view, dataset_id, full_path, file_names=None):
    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(input_token=input_token)

    # no file names were specified (file_names = [''])
    if file_names and not all(file_names): 
        file_names=None
    
    # call getFilesInStorageDataset function
    file_lst = store.getFilesInStorageDataset(datasetId=dataset_id, fileNames=file_names, fullpath=full_path)
    return file_lst
def get_component_requirements(schema_url, source_component, as_graph):
    metadata_model = initalize_metadata_model(schema_url)

    req_components = metadata_model.get_component_requirements(source_component=source_component, as_graph = as_graph)

    return req_components

def download_manifest(input_token, dataset_id, asset_view, as_json, new_manifest_name=''):
    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(input_token=input_token)

    # download existing file
    manifest_data = store.getDatasetManifest(datasetId=dataset_id, downloadFile=True, newManifestName=new_manifest_name)

    #return local file path
    manifest_local_file_path = manifest_data['path']

    # return a json (if as_json = True)
    if as_json: 
        manifest_csv = pd.read_csv(manifest_local_file_path)
        manifest_json = json.loads(manifest_csv.to_json(orient="records"))
        return manifest_json

    return manifest_local_file_path

def get_asset_view_table(input_token, asset_view):
    # call config handler
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(input_token=input_token)

    # get file view table
    file_view_table_df = store.getStorageFileviewTable()

    # convert pandas dataframe to csv
    path = os.getcwd()
    export_path = os.path.join(path, 'tests/data/file_view_table.csv')
    file_view_table_df.to_csv(export_path, index=False)

    return export_path


def get_project_manifests(input_token, project_id, asset_view):
    # use the default asset view from config
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(input_token=input_token)

    # call getprojectManifest function
    lst_manifest = store.getProjectManifests(projectId=project_id)

    return lst_manifest

def get_manifest_datatype(input_token, manifest_id, asset_view):
    # use the default asset view from config
    config_handler(asset_view=asset_view)

    # use Synapse Storage
    store = SynapseStorage(input_token=input_token)

    # get data types of an existing manifest
    manifest_dtypes_dict= store.getDataTypeFromManifest(manifest_id)


    return manifest_dtypes_dict

def get_schema_pickle(schema_url):
    # load schema
    se = SchemaExplorer()

    se.load_schema(schema_url)

    # get schema
    schema_graph = se.get_nx_schema()

    # write to local pickle file
    path = os.getcwd()
    export_path = os.path.join(path, 'tests/data/schema.gpickle')

    with open(export_path, 'wb') as file:
        pickle.dump(schema_graph, file)
    return export_path


def get_subgraph_by_edge_type(schema_url, relationship):
    # use schema generator and schema explorer
    sg = SchemaGenerator(path_to_json_ld=schema_url)
    se = SchemaExplorer()
    se.load_schema(schema_url)

    # get the schema graph 
    schema_graph = se.get_nx_schema()

    # relationship subgraph
    relationship_subgraph = sg.get_subgraph_by_edge_type(schema_graph, relationship)

    # return relationship 
    Arr = []
    for t in relationship_subgraph.edges:
        lst = list(t)
        Arr.append(lst)

    return Arr


def find_class_specific_properties(schema_url, schema_class):
    # use schema explorer
    se = SchemaExplorer()

    # load schema
    se.load_schema(schema_url)

    # return properties
    properties = se.find_class_specific_properties(schema_class)

    return properties