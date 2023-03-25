import os
import shutil
import logging
import pytest

from schematic.manifest.generator import ManifestGenerator
from schematic.schemas.generator import SchemaGenerator
import pandas as pd
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import MagicMock

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



@pytest.fixture(
    params=[
        (True, "Patient"),
        (False, "Patient"),
        (True, "BulkRNA-seqAssay"),
        (False, "BulkRNA-seqAssay"),
    ],
    ids=[
        "use_annotations-Patient",
        "skip_annotations-Patient",
        "use_annotations-BulkRNAseqAssay",
        "skip_annotations-BulkRNAseqAssay",
    ],
)
def manifest_generator(helpers, request):

    # Rename request param for readability
    use_annotations, data_type = request.param

    manifest_generator = ManifestGenerator(
        path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        root=data_type,
        use_annotations=use_annotations,
    )

    yield manifest_generator, use_annotations, data_type

    # Clean-up
    try:
        os.remove(helpers.get_data_path(f"example.{data_type}.schema.json"))
    except FileNotFoundError:
        pass


@pytest.fixture
def simple_test_manifest_excel(helpers):
    yield helpers.get_data_path("mock_manifests/test_bulkRNAseq_manifest.xlsx")

@pytest.fixture
def mock_create_blank_google_sheet():
    'Mock creating a new google sheet'
    er = Mock()
    er.return_value = "mock_spreadsheet_id"
    yield er

@pytest.fixture(params=[True, False], ids=["sheet_url", "data_frame"])
def manifest(dataset_id, manifest_generator, request):

    # Rename request param for readability
    sheet_url = request.param

    # See parameterization of the `manifest_generator` fixture
    generator, use_annotations, data_type = manifest_generator

    manifest = generator.get_manifest(dataset_id=dataset_id, sheet_url=sheet_url)

    yield manifest, use_annotations, data_type, sheet_url


class TestManifestGenerator:

    def test_init(self, helpers):

        generator = ManifestGenerator(
            title="mock_title",
            path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        )

        assert type(generator.title) is str
        # assert generator.sheet_service == mock_creds["sheet_service"]
        assert generator.root is None
        assert type(generator.sg) is SchemaGenerator

    @pytest.mark.google_credentials_needed
    def test_get_manifest_first_time(self, manifest):

        # See parameterization of the `manifest_generator` fixture
        output, use_annotations, data_type, sheet_url = manifest

        if sheet_url:
            logger.debug(output)
            assert isinstance(output, str)
            assert output.startswith("https://docs.google.com/spreadsheets/")
            return

        # Beyond this point, the output is assumed to be a data frame

        # Update expectations based on whether the data type is file-based
        is_file_based = data_type in ["BulkRNA-seqAssay"]

        assert "Component" in output
        assert is_file_based == ("eTag" in output)
        assert is_file_based == ("Filename" in output)
        assert (is_file_based and use_annotations) == ("confidence" in output)

        # Data type-specific columns
        assert (data_type == "Patient") == ("Diagnosis" in output)
        assert (data_type == "BulkRNA-seqAssay") == ("File Format" in output)

        # The rest of the tests have to do with a file-based data type
        if data_type != "BulkRNA-seqAssay":
            assert output.shape[0] == 1  # Number of rows
            return

        # Beyond this point, the output is to be from a file-based assay

        # Confirm contents of Filename column
        assert output["Filename"].tolist() == [
            "TestDataset-Annotations-v3/Sample_A.txt",
            "TestDataset-Annotations-v3/Sample_B.txt",
            "TestDataset-Annotations-v3/Sample_C.txt",
        ]

        # Test dimensions of data frame
        assert output.shape[0] == 3  # Number of rows
        if use_annotations:
            assert output.shape[0] == 3  # Number of rows
            assert "eTag" in output
            assert "confidence" in output
            assert output["Year of Birth"].tolist() == ["1980", "", ""]

        # An annotation merged with an attribute from the data model
        if use_annotations:
            assert output["File Format"].tolist() == ["txt", "csv", "fastq"]
      
    @pytest.mark.parametrize("output_format", [None, "dataframe", "excel", "google_sheet"])
    @pytest.mark.parametrize("sheet_url", [None, True, False])
    @pytest.mark.parametrize("dataset_id", [None, "syn27600056"])
    @pytest.mark.google_credentials_needed
    def test_get_manifest_excel(self, helpers, sheet_url, output_format, dataset_id):
        '''
        Purpose: the goal of this test is to make sure that output_format parameter and sheet_url parameter could function well; 
        In addition, this test also makes sure that getting a manifest with an existing dataset_id is working
        "use_annotations" and "data_type" are hard-coded to fixed values to avoid long run time
        '''

        data_type = "Patient"

        generator = ManifestGenerator(
        path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        root=data_type,
        use_annotations=False,
        )


        manifest= generator.get_manifest(dataset_id=dataset_id, sheet_url = sheet_url, output_format = output_format)

        # if dataset id exists, it could return pandas dataframe, google spreadsheet, or an excel spreadsheet
        if dataset_id: 
            if output_format: 

                if output_format == "dataframe":
                    assert isinstance(manifest, pd.DataFrame)
                elif output_format == "excel":
                    assert os.path.exists(manifest) == True
                else: 
                    assert type(manifest) is str
                    assert manifest.startswith("https://docs.google.com/spreadsheets/")
            else: 
                if sheet_url: 
                    assert type(manifest) is str
                    assert manifest.startswith("https://docs.google.com/spreadsheets/")
                else: 
                    assert isinstance(manifest, pd.DataFrame)
        
        # if dataset id does not exist, it could return an empty google sheet or an empty excel spreadsheet exported from google
        else:
            if output_format: 
                if output_format == "excel":
                    assert os.path.exists(manifest) == True
                else: 
                    assert type(manifest) is str
                    assert manifest.startswith("https://docs.google.com/spreadsheets/")
        
        # Clean-up
        if type(manifest) is str and os.path.exists(manifest): 
            os.remove(manifest)

    # test all the functions used under get_manifest
    @pytest.mark.parametrize("template_id", [["provided", "not provided"]])
    def test_create_empty_manifest_spreadsheet(self, config, manifest_generator, template_id):
        '''
        Create an empty manifest spreadsheet regardless if master_template_id is provided
        Note: _create_empty_manifest_spreadsheet calls _gdrive_copy_file. If there's no template id provided in config, this function will create a new manifest
        '''
        generator, use_annotations, data_type = manifest_generator

        mock_spreadsheet = MagicMock()

        title="Example"

        if template_id == "provided":
            # mock _gdrive_copy_file function 
            with patch('schematic.manifest.generator.ManifestGenerator._gdrive_copy_file') as MockClass:
                instance = MockClass.return_value
                instance.method.return_value = 'mock google sheet id'

                spreadsheet_id = generator._create_empty_manifest_spreadsheet(title=title)
                assert spreadsheet_id == "mock google sheet id"

        else:
            # overwrite test config so that we could test the case when manifest_template_id is not provided
            config["style"]["google_manifest"]["master_template_id"] = ""

            mock_spreadsheet = Mock()
            mock_execute = Mock()


            # Chain the mocks together
            mock_spreadsheet.create.return_value = mock_spreadsheet
            mock_spreadsheet.execute.return_value = mock_execute
            mock_execute.get.return_value = "mock id"
            mock_create = Mock(return_value=mock_spreadsheet)

            with patch.object(generator.sheet_service, "spreadsheets", mock_create):

                spreadsheet_id = generator._create_empty_manifest_spreadsheet(title)
                assert spreadsheet_id == "mock id"


    @pytest.mark.parametrize("wb_headers", [["column one", "column two", "column three"], ["column four", "column two"]])
    @pytest.mark.parametrize("manifest_columns", [["column four"]])
    def test_get_missing_columns(self, manifest_generator, wb_headers, manifest_columns):
        generator, use_annotations, data_type = manifest_generator

        manifest_test_df = pd.DataFrame(columns = manifest_columns)
        missing_columns = generator._get_missing_columns(wb_headers, manifest_test_df)
        if "column four" not in wb_headers:
            assert "column four" in missing_columns 
        else: 
            assert "column four" not in missing_columns

    

    @pytest.mark.parametrize("additional_df_dict", [{'test one column': ['a', 'b'], 'test two column': ['c', 'd']}, None])
    def test_populate_existing_excel_spreadsheet(self, manifest_generator,simple_test_manifest_excel, additional_df_dict):
        generator, use_annotations, data_type = manifest_generator
        if additional_df_dict: 
            additional_test_df = pd.DataFrame(additional_df_dict)
        else: 
            additional_test_df = pd.DataFrame()
        
        # copy the existing excel file
        dummy_output_path = "tests/data/mock_manifests/dummy_output.xlsx"
        shutil.copy(simple_test_manifest_excel, dummy_output_path)

        # added new content to an existing excel spreadsheet if applicable
        generator.populate_existing_excel_spreadsheet(dummy_output_path, additional_test_df)

        # read the new excel spreadsheet and see if columns have been added
        new_df = pd.read_excel(dummy_output_path)

        # if we are not adding any additional content
        if additional_test_df.empty:
            assert len(new_df.columns) == 6
        # we should be able to see new columns get added 
        else: 
            # new columns get added
            assert not new_df[["test one column", "test two column"]].empty

            # remove file
            os.remove(dummy_output_path)



