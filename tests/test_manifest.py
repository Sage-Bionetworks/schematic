import os
import logging
import pytest

from schematic.manifest.generator import ManifestGenerator
from schematic.schemas.generator import SchemaGenerator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_creds():
    mock_creds = {
        'sheet_service': 'mock_sheet_service',
        'drive_service': 'mock_drive_service',
        'creds': 'mock_creds'
    }
    yield mock_creds


@pytest.fixture(params=[True, False], ids=["use_annotations", "skip_annotations"])
def manifest_generator(helpers, request):

    # Rename request param for readability
    use_annotations = request.param

    manifest_generator = ManifestGenerator(
        path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        root="Patient",
        use_annotations=use_annotations,
    )

    yield manifest_generator, use_annotations

    # Clean-up
    try:
        os.remove(helpers.get_data_path("example.Patient.schema.json"))
    except FileNotFoundError:
        pass


@pytest.fixture(params=[True, False], ids=["sheet_url", "data_frame"])
def manifest(manifest_generator, request):

    # Rename request param for readability
    sheet_url = request.param

    # See parameterization of the `manifest_generator` fixture
    generator, use_annotations = manifest_generator

    manifest = generator.get_manifest(
        dataset_id="syn25057021",
        sheet_url=sheet_url
    )

    yield manifest, use_annotations, sheet_url



class TestManifestGenerator:

    def test_init(self, monkeypatch, mock_creds, helpers):

        monkeypatch.setattr("schematic.manifest.generator.build_credentials",
                            lambda: mock_creds)

        generator = ManifestGenerator(
            title="mock_title",
            path_to_json_ld=helpers.get_data_path("example.model.jsonld")
        )

        assert type(generator.title) is str
        assert generator.sheet_service == mock_creds["sheet_service"]
        assert generator.root is None
        assert type(generator.sg) is SchemaGenerator


    @pytest.mark.google_credentials_needed
    def test_get_manifest_first_time(self, manifest):

        # See parameterization of the `manifest_generator` fixture
        output, use_annotations, sheet_url = manifest

        if sheet_url:
            assert isinstance(output, str)
            assert output.startswith("https://docs.google.com/spreadsheets/")
            print(output)
            return

        # Beyond this point, the output is assumed to be a data frame
        assert "Year of Birth" in output

        if use_annotations:
            assert output.shape[1] == 13  # Number of columns
            assert output.shape[0] == 3  # Number of rows
            assert "eTag" in output
            assert "confidence" in output
            assert output["Year of Birth"].tolist() == ["1980", "", ""]
        else:
            assert output.shape[1] == 6  # Number of columns
            assert output.shape[0] == 0  # Number of rows
            assert "confidence" not in output
            assert output["Year of Birth"].tolist() == []
