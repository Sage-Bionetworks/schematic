import logging
import os
import shutil
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from schematic.configuration.configuration import Configuration
from schematic.manifest.generator import ManifestGenerator
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_parser import DataModelParser
from schematic.utils.google_api_utils import execute_google_api_requests
from schematic_api.api import create_app

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def generate_graph_data_model(helpers, path_to_data_model, data_model_labels):
    """
    Simple helper function to generate a networkx graph data model from a CSV or JSONLD data model
    """

    # Instantiate Parser
    data_model_parser = DataModelParser(path_to_data_model=path_to_data_model)

    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    return graph_data_model


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

    path_to_data_model = helpers.get_data_path("example.model.jsonld")

    # Get graph data model
    graph_data_model = generate_graph_data_model(
        helpers,
        path_to_data_model=path_to_data_model,
        data_model_labels="class_label",
    )

    manifest_generator = ManifestGenerator(
        path_to_data_model=path_to_data_model,
        graph=graph_data_model,
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
def simple_manifest_generator(manifest_generator):
    generator, use_annotations, data_type = manifest_generator
    yield generator


@pytest.fixture
def simple_test_manifest_excel(helpers):
    yield helpers.get_data_path("mock_manifests/test_bulkRNAseq_manifest.xlsx")


@pytest.fixture
def mock_create_blank_google_sheet():
    "Mock creating a new google sheet"
    er = Mock()
    er.return_value = "mock_spreadsheet_id"
    yield er


@pytest.fixture(params=[True, False], ids=["sheet_url", "data_frame"])
def manifest(dataset_id, manifest_generator, request):
    """
    Only seems to be used for TestManifestGenerator.test_get_manifest_first_time
    """
    # Rename request param for readability
    sheet_url = request.param

    # See parameterization of the `manifest_generator` fixture
    generator, use_annotations, data_type = manifest_generator

    manifest = generator.get_manifest(dataset_id=dataset_id, sheet_url=sheet_url)

    yield manifest, use_annotations, data_type, sheet_url


@pytest.fixture(scope="class")
def app():
    app = create_app()
    yield app


class TestManifestGenerator:
    def test_init(self, helpers):
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        generator = ManifestGenerator(
            graph=graph_data_model,
            title="mock_title",
            path_to_data_model=path_to_data_model,
            root="Patient",
        )

        assert type(generator.title) is str
        # assert generator.sheet_service == mock_creds["sheet_service"]
        assert generator.root is "Patient"
        assert type(generator.dmge) is DataModelGraphExplorer

    @pytest.mark.parametrize(
        "data_type, exc, exc_message",
        [
            (
                "MissingComponent",
                LookupError,
                "could not be found in the data model schema",
            ),
            (None, ValueError, "No DataType has been provided."),
        ],
        ids=["DataType not found in Schema", "No DataType provided"],
    )
    def test_missing_root_error(self, helpers, data_type, exc, exc_message):
        """
        Test for errors when either no DataType is provided or when a DataType is provided but not found in the schema
        """
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        # A LookupError should be raised and include message when the component cannot be found
        with pytest.raises(exc) as e:
            generator = ManifestGenerator(
                path_to_data_model=helpers.get_data_path("example.model.jsonld"),
                graph=graph_data_model,
                root=data_type,
                use_annotations=False,
            )

        # Check message contents
        assert exc_message in str(e)

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
        assert is_file_based == ("entityId" in output)
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
            "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_A.txt",
            "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_B.txt",
            "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_C.txt",
        ]

        # Test dimensions of data frame
        assert output.shape[0] == 3  # Number of rows
        if use_annotations:
            assert output.shape[0] == 3  # Number of rows
            assert "entityId" in output
            assert "confidence" in output
            assert output["Year of Birth"].tolist() == ["1980", "", ""]

        # An annotation merged with an attribute from the data model
        if use_annotations:
            assert output["File Format"].tolist() == ["txt", "csv", "fastq"]

    @pytest.mark.parametrize(
        "output_format", [None, "dataframe", "excel", "google_sheet"]
    )
    @pytest.mark.parametrize("sheet_url", [None, True, False])
    @pytest.mark.parametrize("dataset_id", [None, "syn27600056"])
    @pytest.mark.google_credentials_needed
    def test_get_manifest_excel(self, helpers, sheet_url, output_format, dataset_id):
        """
        Purpose: the goal of this test is to make sure that output_format parameter and sheet_url parameter could function well;
        In addition, this test also makes sure that getting a manifest with an existing dataset_id is working
        "use_annotations" and "data_type" are hard-coded to fixed values to avoid long run time
        """

        data_type = "Patient"

        # Get path to data model
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            root=data_type,
            use_annotations=False,
        )

        manifest = generator.get_manifest(
            dataset_id=dataset_id, sheet_url=sheet_url, output_format=output_format
        )

        # if dataset id exists, it could return pandas dataframe, google spreadsheet, or an excel spreadsheet
        if dataset_id:
            if output_format == "dataframe":
                assert isinstance(manifest, pd.DataFrame)
            elif output_format == "excel":
                assert os.path.exists(manifest) == True
            elif sheet_url:
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

    @pytest.mark.parametrize(
        "dataset_id",
        [("syn27600056"), ("syn52397659")],
        ids=["Annotations present", "Annotations not present"],
    )
    def test_get_manifest_no_annos(self, helpers, dataset_id):
        """
        Test to cover manifest generation under the case where use_annotations is True
        but there are no annotations in the dataset
        """

        # Use a non-file based DataType
        data_type = "Patient"

        # Get path to data model
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        # Instantiate object with use_annotations set to True
        generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            root=data_type,
            use_annotations=True,
        )

        # Get manifest as a dataframe
        manifest = generator.get_manifest(
            dataset_id=dataset_id, sheet_url=False, output_format="dataframe"
        )

        # Case where annotations are present in the dataset
        # manifest should have pulled in the annotations
        if dataset_id == "syn27600056":
            assert manifest["Patient ID"].size > 1

        # Case where annotations are not present in the dataset
        # manifest should be empty (One column will have the component filled so not truly empty)
        elif dataset_id == "syn52397659":
            assert manifest["Patient ID"].size == 1

    # test all the functions used under get_manifest
    def test_create_empty_manifest_spreadsheet(self, simple_manifest_generator):
        """
        Create an empty manifest spreadsheet.
        Note: _create_empty_manifest_spreadsheet calls _gdrive_copy_file.
        """
        generator = simple_manifest_generator
        title = "Example"

        # mock _gdrive_copy_file function
        with patch(
            "schematic.manifest.generator.ManifestGenerator._gdrive_copy_file",
            return_value="mock google sheet id",
        ):
            spreadsheet_id = generator._create_empty_manifest_spreadsheet(title=title)
            assert spreadsheet_id == "mock google sheet id"

    @pytest.mark.parametrize("schema_path_provided", [True, False])
    def test_get_json_schema(
        self, simple_manifest_generator, helpers, schema_path_provided
    ):
        """
        Open json schema as a dictionary
        """
        generator = simple_manifest_generator

        if schema_path_provided:
            json_schema_path = helpers.get_data_path("example.model.jsonld")
            json_schema = generator._get_json_schema(
                json_schema_filepath=json_schema_path
            )

        else:
            mock_json_schema = Mock()
            mock_json_schema.return_value = "mock json ld"
            with patch.object(
                DataModelJSONSchema, "get_json_validation_schema", mock_json_schema
            ):
                json_schema = generator._get_json_schema(json_schema_filepath=None)
                assert json_schema == "mock json ld"

            assert type(json_schema) == str

    def test_gather_all_fields(self, simple_manifest_generator):
        """
        gather all fields is a wrapper around three functions: _get_required_metadata_fields, _gather_dependency_requirements
        and _get_additional_metadata
        """
        generator = simple_manifest_generator

        with patch(
            "schematic.manifest.generator.ManifestGenerator._get_required_metadata_fields"
        ) as MockClass:
            MockClass.return_value = "mock required metadata fields"
            with patch(
                "schematic.manifest.generator.ManifestGenerator._gather_dependency_requirements"
            ) as MockRequirement:
                MockRequirement.return_value = "mock required metadata fields"
                with patch(
                    "schematic.manifest.generator.ManifestGenerator._get_additional_metadata"
                ) as MockAdditionalData:
                    MockAdditionalData.return_value = "mock required metadata fields"
                    required_metadata = generator._gather_all_fields(
                        "mock fields", "mock json schema"
                    )

                    assert required_metadata == "mock required metadata fields"

    # test: test and make sure root node as a metadata component get added in additional_metadata dictionary
    # test cases: 1) component as Patient; 2) component as BulkRNA-seqAssay
    # assume there is no existing additional metadata
    @pytest.mark.parametrize(
        "data_type,required_metadata_fields,expected",
        [
            ("Patient", {"Component": []}, {"Component": ["Patient"]}),
            (
                "BulkRNA-seqAssay",
                {"Filename": [], "Component": []},
                {"Component": ["BulkRNA-seqAssay"]},
            ),
        ],
    )
    def test_add_root_to_component_without_additional_metadata(
        self, helpers, data_type, required_metadata_fields, expected
    ):
        # Get path to data model
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        manifest_generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            root=data_type,
        )
        manifest_generator._add_root_to_component(required_metadata_fields)
        assert manifest_generator.additional_metadata == expected

    # test and make sure root node as a metadata component get added in additional_metadata dictionary
    # also make sure that length entry of column Component is the same as length of entry of column Filename
    # test cases: different length of Filename column in additional metadata
    # assume there is additional metadata
    @pytest.mark.parametrize(
        "additional_metadata",
        [
            {
                "author": [
                    "test",
                    "",
                ],
                "Filename": ["test.txt", "test2.txt"],
                "Component": [],
            },
            {"Year of Birth": ["1988"], "Filename": ["test.txt"], "Component": []},
        ],
    )
    def test_add_root_to_component_with_additional_metadata(
        self, helpers, additional_metadata
    ):
        # Get path to data model
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        manifest_generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            root="BulkRNA-seqAssay",
        )

        # add mock additional metadata
        manifest_generator.additional_metadata = additional_metadata

        mock_required_metadata_fields = {"Filename": [], "Component": []}
        manifest_generator._add_root_to_component(mock_required_metadata_fields)
        assert len(manifest_generator.additional_metadata["Component"]) == len(
            additional_metadata["Filename"]
        )

    # TO DO: add tests for: test_create_empty_gs

    @pytest.mark.parametrize(
        "wb_headers",
        [["column one", "column two", "column three"], ["column four", "column two"]],
    )
    @pytest.mark.parametrize("manifest_columns", [["column four"]])
    def test_get_missing_columns(
        self, simple_manifest_generator, wb_headers, manifest_columns
    ):
        generator = simple_manifest_generator

        manifest_test_df = pd.DataFrame(columns=manifest_columns)
        manifest_test_df_headers = list(manifest_test_df.columns)
        out_of_schema_columns = generator._get_missing_columns(
            manifest_test_df_headers, wb_headers
        )

        if "column four" not in wb_headers:
            assert "column four" in out_of_schema_columns
        else:
            assert "column four" not in out_of_schema_columns

    # Need to actually put in different dfs
    @pytest.mark.parametrize(
        "existing_manifest",
        [
            {
                "Patient ID": ["1738"],
                "Sex": ["Male"],
                "Year of Birth": ["1999"],
                "Diagnosis": [""],
                "Component": [""],
                "Cancer Type": [""],
                "Family History": [""],
            },
            {
                "Patient ID": ["1738"],
                "Sex": ["Male"],
                "Year of Birth": ["1999"],
                "Diagnosis": [""],
                "Component": [""],
                "Cancer Type": [""],
                "Family History": [""],
                "Non Schema Column": [""],
            },
            {"Patient ID": ["1738"], "Sex": ["Male"]},
            None,
        ],
    )
    @pytest.mark.google_credentials_needed
    def test_update_dataframe_with_existing_df(self, helpers, existing_manifest):
        """
        Tests the following discrepancies with an existing schema:
            - schema has matching columns to existing_df
            - existing_df has columns the schema does not
            - schema has columns the existing_df does not.
            - No existing manifest
        """
        data_type = "Patient"
        sheet_url = True

        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # Get graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        # Instantiate the Manifest Generator.
        generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            root=data_type,
            use_annotations=False,
        )

        # Generate a google sheet url for a blank manifest.
        empty_manifest_url = generator.get_manifest(sheet_url=sheet_url)

        # Loading existing manifest
        existing_manifest_df = pd.DataFrame(existing_manifest)

        # Update the empty manifest with the existing manifest
        updated_df = generator._update_dataframe_with_existing_df(
            empty_manifest_url=empty_manifest_url,
            existing_df=existing_manifest_df,
        )[0]

        # Check that update happened as intended.
        # If the existing manifest is emtpy, the columns will not change, no data will be added
        if existing_manifest_df.empty:
            assert updated_df.empty == True
            assert list(updated_df.columns) == [
                "Patient ID",
                "Sex",
                "Year of Birth",
                "Diagnosis",
                "Component",
                "Cancer Type",
                "Family History",
            ]
        # If the existing manifest has only 2 of the schema columns, the columns should match the schema, data is added.
        elif len(existing_manifest_df.columns) == 2:
            assert updated_df["Patient ID"][0] == "1738"
            assert list(updated_df.columns) == [
                "Patient ID",
                "Sex",
                "Year of Birth",
                "Diagnosis",
                "Component",
                "Cancer Type",
                "Family History",
            ]
        # If the existing manifest has matching columns to the schema, the columns should remain the same, data is added.
        elif len(existing_manifest_df.columns) == 7:
            assert updated_df["Patient ID"][0] == "1738"
            assert list(updated_df.columns) == [
                "Patient ID",
                "Sex",
                "Year of Birth",
                "Diagnosis",
                "Component",
                "Cancer Type",
                "Family History",
            ]
        # If the existing manifest has an extra column that is not in the schema, the new column should be added, data is added.
        elif len(existing_manifest_df.columns) == 8:
            assert updated_df["Patient ID"][0] == "1738"
            assert list(updated_df.columns) == [
                "Patient ID",
                "Sex",
                "Year of Birth",
                "Diagnosis",
                "Component",
                "Cancer Type",
                "Family History",
                "Non Schema Column",
            ]

    @pytest.mark.parametrize(
        "additional_df_dict",
        [
            {
                "Filename": ["a", "b"],
                "Sample ID": ["a", "b"],
                "File Format": ["a", "b"],
                "Component": ["a", "b"],
                "Genome Build": ["a", "b"],
                "Genome FASTA": ["a", "b"],
                "test_one_column": ["a", "b"],
                "test_two_column": ["c", "d"],
            },
            None,
        ],
    )
    def test_populate_existing_excel_spreadsheet(
        self, simple_manifest_generator, simple_test_manifest_excel, additional_df_dict
    ):
        generator = simple_manifest_generator
        if additional_df_dict:
            additional_test_df = pd.DataFrame(additional_df_dict)
        else:
            additional_test_df = pd.DataFrame()

        # copy the existing excel file
        dummy_output_path = "tests/data/mock_manifests/dummy_output.xlsx"
        shutil.copy(simple_test_manifest_excel, dummy_output_path)

        # added new content to an existing excel spreadsheet if applicable
        generator.populate_existing_excel_spreadsheet(
            dummy_output_path, additional_test_df
        )

        # read the new excel spreadsheet and see if columns have been added
        new_df = pd.read_excel(dummy_output_path)

        # if we are not adding any additional content
        if additional_test_df.empty:
            # make sure that new content also gets added
            assert len(new_df.columns) == 6
        # we should be able to see new columns get added
        else:
            # new columns get added
            assert not new_df[["test_one_column", "test_two_column"]].empty
            assert len(new_df.test_one_column.value_counts()) > 0
            assert len(new_df.test_two_column.value_counts()) > 0

        # remove file
        os.remove(dummy_output_path)

    @pytest.mark.parametrize(
        "return_output", ["Mock excel file path", "Mock google sheet link"]
    )
    def test_create_single_manifest(
        self, simple_manifest_generator, helpers, return_output
    ):
        with patch(
            "schematic.manifest.generator.ManifestGenerator.get_manifest",
            return_value=return_output,
        ):
            json_ld_path = helpers.get_data_path("example.model.jsonld")
            data_type = "Patient"

            graph_data_model = generate_graph_data_model(
                helpers,
                path_to_data_model=json_ld_path,
                data_model_labels="class_label",
            )

            result = simple_manifest_generator.create_single_manifest(
                path_to_data_model=json_ld_path,
                graph_data_model=graph_data_model,
                data_type=data_type,
                output_format="google_sheet",
                use_annotations=False,
            )
            assert result == return_output

    @pytest.mark.parametrize(
        "test_data_types", [["Patient", "Biospecimen"], ["all manifests"]]
    )
    def test_create_manifests_raise_errors(
        self, simple_manifest_generator, helpers, test_data_types
    ):
        with pytest.raises(ValueError) as exception_info:
            json_ld_path = helpers.get_data_path("example.model.jsonld")
            data_types = test_data_types
            dataset_ids = ["syn123456"]

            simple_manifest_generator.create_manifests(
                path_to_data_model=json_ld_path,
                data_types=data_types,
                dataset_ids=dataset_ids,
                output_format="google_sheet",
                use_annotations=False,
                data_model_labels="class_label",
            )

    @pytest.mark.parametrize(
        "test_data_types, dataset_ids, expected_result",
        [
            (
                ["Patient", "Biospecimen"],
                ["mock dataset id1", "mock dataset id2"],
                ["mock google sheet link", "mock google sheet link"],
            ),
            (["Patient"], ["mock dataset id1"], ["mock google sheet link"]),
        ],
    )
    def test_create_manifests(
        self,
        simple_manifest_generator,
        helpers,
        test_data_types,
        dataset_ids,
        expected_result,
    ):
        with patch(
            "schematic.manifest.generator.ManifestGenerator.create_single_manifest",
            return_value="mock google sheet link",
        ):
            json_ld_path = helpers.get_data_path("example.model.jsonld")
            all_results = simple_manifest_generator.create_manifests(
                path_to_data_model=json_ld_path,
                data_types=test_data_types,
                dataset_ids=dataset_ids,
                output_format="google_sheet",
                use_annotations=False,
                data_model_labels="class_label",
            )
            assert all_results == expected_result

    @pytest.mark.parametrize(
        "component, datasetId, expected_file_based, expected_rows, expected_files, expected_annotations",
        [
            (
                "BulkRNA-seqAssay",
                "syn61374924",
                True,
                4,
                pd.Series(
                    [
                        "schematic - main/BulkRNASeq and files/txt1.txt",
                        "schematic - main/BulkRNASeq and files/txt2.txt",
                        "schematic - main/BulkRNASeq and files/txt4.txt",
                        "schematic - main/BulkRNASeq and files/txt3.txt",
                    ],
                    name="Filename",
                ),
                {
                    "File Format": pd.Series(
                        ["BAM", "CRAM", "CSV/TSV", ""], name="File Format"
                    ),
                    "Genome Build": pd.Series(
                        ["GRCh37", "GRCh38", "GRCm38", ""], name="Genome Build"
                    ),
                },
            ),
            (
                "BulkRNA-seqAssay",
                "syn25614635",
                True,
                3,
                pd.Series(
                    [
                        "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_A.txt",
                        "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_B.txt",
                        "schematic - main/TestDatasets/TestDataset-Annotations-v3/Sample_C.txt",
                    ],
                    name="Filename",
                ),
                {
                    "File Format": pd.Series(
                        ["txt", "csv", "fastq"], name="File Format"
                    ),
                    "Year of Birth": pd.Series(["1980", "", ""], name="Year of Birth"),
                },
            ),
        ],
    )
    @pytest.mark.parametrize("use_annotations", [True, False])
    def test_get_manifest_with_files(
        self,
        helpers,
        component,
        datasetId,
        expected_file_based,
        expected_rows,
        expected_files,
        expected_annotations,
        use_annotations,
    ):
        """
        Test to ensure that
            when generating a record based manifset that has files in the dataset that the files are not added to the manifest as well
            when generating a file based manifest from a dataset thathas had files added that the files are added correctly
        """
        # GIVEN the example data model
        path_to_data_model = helpers.get_data_path("example.model.jsonld")

        # AND a graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            path_to_data_model=path_to_data_model,
            data_model_labels="class_label",
        )

        # AND a manifest generator
        generator = ManifestGenerator(
            path_to_data_model=path_to_data_model,
            graph=graph_data_model,
            root=component,
            use_annotations=use_annotations,
        )

        # WHEN a filebased manifest is generated for the appropriate dataset as a dataframe
        manifest = generator.get_manifest(
            dataset_id=datasetId, output_format="dataframe"
        )

        # AND the number of rows are checked
        n_rows = manifest.shape[0]

        # THEN the manifest should have the expected number of rows
        assert n_rows == expected_rows

        # AND the manifest should have the columns expected of filebased metadata
        assert "Filename" in manifest.columns
        assert "entityId" in manifest.columns

        # AND the manifest should have the expected files from the dataset
        assert_series_equal(
            manifest["Filename"],
            expected_files,
            check_dtype=False,
        )

        # AND if annotations are used to generate the manifest
        if use_annotations:
            # THEN the annotations in the generated manifest should match the expected annotations
            for attribute, annotations in expected_annotations.items():
                assert_series_equal(
                    manifest[attribute],
                    annotations,
                    check_dtype=False,
                )
