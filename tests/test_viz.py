from io import StringIO
import json
import os
import pandas as pd
import logging

import pytest

from schematic.visualization.attributes_explorer import AttributesExplorer
from schematic.visualization.tangled_tree import TangledTree

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def attributes_explorer(helpers):

    # Get JSONLD file path
    path_to_jsonld = helpers.get_data_path("example.model.jsonld")
    
    # Initialize TangledTree
    attributes_explorer = AttributesExplorer(path_to_jsonld)
    yield attributes_explorer

@pytest.fixture
def tangled_tree(helpers):
    figure_type = 'component'

    # Get JSONLD file path
    path_to_jsonld = helpers.get_data_path("example.model.jsonld")
    
    # Initialize TangledTree
    tangled_tree = TangledTree(path_to_jsonld, figure_type)
    yield tangled_tree

class TestVisualization:
    def test_ae(self, helpers, attributes_explorer):
        attributes_str = attributes_explorer.parse_attributes(save_file=False)
        
        df = pd.read_csv(StringIO(attributes_str)).drop(columns=['Unnamed: 0'])

        # For the attributes df define expected columns
        expect_col_names = ['Attribute', 'Label', 'Description',
                            'Required', 'Cond_Req', 'Valid Values', 'Conditional Requirements',
                            'Component']
        expected_components = ['Biospecimen', 'Patient', 'BulkRNA-seqAssay',
                               'ScRNA-seqLevel1', 'ScRNA-seqLevel2', 'ScRNA-seqLevel3',
                               'ScRNA-seqLevel4', 'ImagingChannels', 'Imaging']
        
        # Get actual values
        actual_column_names = df.columns.tolist()
        actual_components = df.loc[df['Attribute']== 'Component']['Component'].tolist()

        assert actual_column_names == expect_col_names
        assert actual_components == expected_components

    def test_text(self, helpers, tangled_tree):
        text_format = 'plain'

        # Get text for tangled tree.
        text_str = tangled_tree.get_text_for_tangled_tree(text_format, save_file=False)
        
        df= pd.read_csv(StringIO(text_str)).drop(columns=['Unnamed: 0'])
        
        # Define expected text associated with 'Patient' and 'Imaging' tree
        expected_patient_text = ['Biospecimen', 'BulkRNA-seqAssay', 'ScRNA-seqLevel1', 'ScRNA-seqLevel2',
                                 'ScRNA-seqLevel3', 'ScRNA-seqLevel4', 'ImagingChannels', 'Imaging']

        expected_imaging_text = ['Biospecimen', 'Patient', 'BulkRNA-seqAssay', 'ScRNA-seqLevel1',
                                 'ScRNA-seqLevel2', 'ScRNA-seqLevel3', 'ScRNA-seqLevel4', 'ImagingChannels']
        
        # Get actual text
        actual_patient_text = df.loc[df['Component'] == 'Patient']['name'].tolist()

        actual_imaging_text = df.loc[df['Component'] == 'Imaging']['name'].tolist()
        
        # Check some random pieces of text we would assume to be in the plain text.
        assert ((df['Component'] == 'Patient') & (df['name'] == 'Biospecimen')).any()
        
        # Check the extracted text matches expected text.
        assert actual_patient_text == expected_patient_text
        assert actual_imaging_text == expected_imaging_text

    def test_layers(self, helpers, tangled_tree):
        layers_str = tangled_tree.get_tangled_tree_layers(save_file=False)[0]

        # Define what we expect the layers list to be.
        expected_layers_list = [
                                    [
                                        {'id': 'Patient'},
                                    ],
                                    [
                                        {'id': 'Biospecimen', 'parents': ['Patient']},
                                        {'id': 'Imaging'},
                                    ],
                                    [
                                        {'id': 'ScRNA-seqLevel1', 'parents': ['Biospecimen']},
                                        {'id': 'BulkRNA-seqAssay', 'parents': ['Biospecimen']},
                                        {'id': 'ImagingChannels', 'parents': ['Imaging', 'Biospecimen']},
                                    ],
                                    [
                                        {'id': 'ScRNA-seqLevel2', 'parents': ['ScRNA-seqLevel1']},
                                    ],
                                    [
                                        {'id': 'ScRNA-seqLevel3', 'parents': ['ScRNA-seqLevel2']},
                                        ],
                                    [
                                        {'id': 'ScRNA-seqLevel4', 'parents': ['ScRNA-seqLevel3']},
                                    ],
                                ]

        # Get actual layers.
        actual_layers_list = json.loads(layers_str)
        
        # Check.
        assert actual_layers_list == expected_layers_list
