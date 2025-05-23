import pytest

from pathlib import Path
import os
import collections
from shutil import rmtree

from schematic.schemas.json_schema_component_generator import (
    JsonSchemaGeneratorDirector,
    JsonSchemaComponentGenerator,
)
from tests.utils import json_files_equal
from unittest.mock import patch

OutputDirectory = collections.namedtuple("OutputDirectory", ["given", "expected"])


@pytest.fixture
def data_model_location(helpers, request):
    try:
        data_model_location = request.param
    except AttributeError:
        data_model_location = helpers.get_data_path("tests/data/example.model.jsonld")

    yield data_model_location


@pytest.fixture
def output_directory(helpers, data_model_location):
    data_model_basename = Path(data_model_location).stem

    given_output_directory = Path(helpers.get_data_path("test_jsonschemas"))
    expected_output_directory = Path(
        helpers.get_data_path("test_jsonschemas"), data_model_basename
    )

    if not os.path.exists(given_output_directory):
        os.makedirs(given_output_directory)

    yield OutputDirectory(
        given=given_output_directory, expected=expected_output_directory
    )

    # Cleanup after test
    rmtree(given_output_directory)


class TestJsonSchemaGeneratorDirector:
    def test_init(self, output_directory, example_data_model_path):
        # GIVEN the JsonSchemaGeneratorDirector class and certain parameters
        # WHEN the class is initialized
        generator = JsonSchemaGeneratorDirector(
            data_model_location=example_data_model_path,
            components=["MockComponent"],
            output_directory=output_directory.given,
        )

        # THEN the class should be initialized with the correct parameters
        assert generator.data_model_location == example_data_model_path
        assert generator.components == ["MockComponent"]
        assert generator.output_directory == output_directory.given

    @pytest.mark.parametrize(
        "specified_component, expected_components",
        [
            (["MockComponent"], ["MockComponent"]),
            (
                None,
                [
                    "Patient",
                    "Biospecimen",
                    "Bulk RNA-seq Assay",
                    "MockComponent",
                    "MockRDB",
                    "MockFilename",
                    "JSONSchemaComponent"
                ],
            ),
        ],
    )
    def test_generate_jsonschema(
        self,
        helpers,
        specified_component,
        expected_components,
        output_directory,
        example_data_model_path,
        mocker,
    ):
        component_gather_spy = mocker.spy(
            JsonSchemaGeneratorDirector, "gather_components"
        )

        # GIVEN a JsonSchemaGeneratorDirector instance, a data model, an output directory, and optionally a component
        generator = JsonSchemaGeneratorDirector(
            data_model_location=example_data_model_path,
            components=specified_component,
            output_directory=output_directory.given,
        )

        # WHEN a schema is generated
        json_schema = generator.generate_jsonschema()

        # AND if no component is specified already
        # THEN all components should be gathered from the data model
        if specified_component is None:
            component_gather_spy.assert_called_once()

        # AND a list json schema(s) should be returned with entries for each component
        assert json_schema is not None
        assert isinstance(json_schema, list)
        assert len(json_schema) == len(expected_components)

        # AND for each component generated
        for expected_component in expected_components:
            expected_component = expected_component.replace(" ", "")

            expected_jsonschema = helpers.get_data_path(
                f"expected_jsonschemas/expected.{expected_component}.schema.json"
            )

            generated_jsonschema = helpers.get_data_path(
                f"{output_directory.expected}/{expected_component}_validation_schema.json"
            )

            # A file should be written to the correct path
            assert os.path.isfile(generated_jsonschema)
            # AND the JSON schema should match the expected schema
            assert json_files_equal(expected_jsonschema, generated_jsonschema)

    @pytest.mark.parametrize(
        "data_model_location, expected_components",
        [
            (
                "example.model.jsonld",
                [
                    "Patient",
                    "Biospecimen",
                    "Bulk RNA-seq Assay",
                    "MockComponent",
                    "MockRDB",
                    "MockFilename",
                ],
            ),
            (
                "https://raw.githubusercontent.com/ncihtan/data-models/238173f2f193d0b068313d096fcab6ee19c34c3b/HTAN.model.jsonld",
                [
                    "10x Visium Spatial Transcriptomics - Auxiliary Files",
                    "10x Visium Spatial Transcriptomics - RNA-seq Level 1",
                    "10x Visium Spatial Transcriptomics - RNA-seq Level 2",
                    "10x Visium Spatial Transcriptomics - RNA-seq Level 3",
                    "10x Visium Spatial Transcriptomics - RNA-seq Level 4",
                    "10X Genomics Xenium ISS Experiment",
                    "Bulk Methylation-seq Level 1",
                    "Bulk Methylation-seq Level 2",
                    "Bulk Methylation-seq Level 3",
                    "Bulk RNA-seq Level 1",
                    "Bulk RNA-seq Level 2",
                    "Bulk RNA-seq Level 3",
                    "Bulk WES Level 1",
                    "Bulk WES Level 2",
                    "CDS Sequencing Template",
                    "Bulk WES Level 3",
                    "Electron Microscopy Level 1",
                    "Electron Microscopy Level 2",
                    "Electron Microscopy Level 3",
                    "Electron Microscopy Level 4",
                    "ExSeq Minimal",
                    "HI-C-seq Level 1",
                    "HI-C-seq Level 2",
                    "HI-C-seq Level 3",
                    "HTAN RPPA Antibody Table",
                    "Imaging Level 1",
                    "Imaging Level 2",
                    "Imaging Level 3 Segmentation",
                    "Imaging Level 4",
                    "Mass Spectrometry Level 1",
                    "Mass Spectrometry Level 2",
                    "Mass Spectrometry Level 3",
                    "Mass Spectrometry Level 4",
                    "Microarray Level 1",
                    "Microarray Level 2",
                    "Multiplexed CITE-seq Level 1",
                    "Multiplexed CITE-seq Level 2",
                    "Multiplexed CITE-seq Level 3",
                    "Multiplexed CITE-seq Level 4",
                    "Nanostring CosMx SMI Experiment",
                    "NanoString GeoMx DSP Spatial Transcriptomics Level 1",
                    "NanoString GeoMx DSP Spatial Transcriptomics Level 3",
                    "Other Assay",
                    "RPPA Level 2",
                    "RPPA Level 3",
                    "RPPA Level 4",
                    "SRRS Imaging Level 2",
                    "Slide-seq Level 1",
                    "Slide-seq Level 2",
                    "Slide-seq Level 3",
                    "scATAC-seq Level 1",
                    "scATAC-seq Level 2",
                    "scATAC-seq Level 3",
                    "scATAC-seq Level 4",
                    "scDNA-seq Level 1",
                    "scDNA-seq Level 2",
                    "scRNA-seq Level 1",
                    "scRNA-seq Level 2",
                    "scRNA-seq Level 3",
                    "scRNA-seq Level 4",
                    "scmC-seq Level 1",
                    "scmC-seq Level 2",
                    "Accessory Manifest",
                    "Acute Lymphoblastic Leukemia Tier 3",
                    "Biospecimen",
                    "Breast Cancer Tier 3",
                    "Clinical Data Tier 2",
                    "Colorectal Cancer Tier 3",
                    "Demographics",
                    "Diagnosis",
                    "Exposure",
                    "Family History",
                    "Follow Up",
                    "Lung Cancer Tier 3",
                    "Melanoma Tier 3",
                    "Molecular Test",
                    "NanoString GeoMx DSP ROI DCC Segment Annotation Metadata",
                    "NanoString GeoMx DSP ROI RCC Segment Annotation Metadata",
                    "Neuroblastoma and Glioma Tier 3",
                    "Ovarian Cancer Tier 3",
                    "Pancreatic Cancer Tier 3",
                    "Participant Vital Status Update",
                    "Precancer Diagnosis",
                    "Prostate Cancer Tier 3",
                    "Publication Manifest",
                    "SRRS Biospecimen",
                    "SRRS Clinical Data Tier 2",
                    "Sarcoma Tier 3",
                    "Therapy",
                ],
            ),
        ],
        ids=[
            "example model - local",
            "HTAN model - url",
        ],
    )
    def test_gather_components(
        self, helpers, data_model_location: str, expected_components: list[str]
    ):
        # GIVEN a data model from a local file or a url
        if data_model_location.startswith("example"):
            data_model_location = helpers.get_data_path(data_model_location)

        # WHEN an instance of the JsonSchemaGeneratorDirector class is created
        generator = JsonSchemaGeneratorDirector(data_model_location=data_model_location)
        # AND the gather_components method is called
        identified_components = generator.gather_components()

        # THEN all components from the data model should be identified
        assert expected_components.sort() == identified_components.sort()


class TestJsonSchemaComponentGenerator:
    @pytest.mark.parametrize(
        "data_model_location, component",
        [
            ("example.model.jsonld", "MockComponent"),
            (
                "https://raw.githubusercontent.com/ncihtan/data-models/238173f2f193d0b068313d096fcab6ee19c34c3b/HTAN.model.jsonld",
                "Biospecimen",
            ),
        ],
        indirect=["data_model_location"],
    )
    def test_init(
        self,
        data_model_location,
        component,
        parsed_example_model,
        output_directory,
    ):
        # GIVEN certain parameters
        expected_output_path = Path(
            output_directory.expected, f"{component}_validation_schema.json"
        )

        # WHEN the JsonSchemaComponentGenerator class is initialized
        generator = JsonSchemaComponentGenerator(
            data_model_location=data_model_location,
            component=component,
            output_directory=output_directory.given,
            parsed_model=parsed_example_model,
        )

        # THEN the class should be initialized with the correct parameters
        assert generator.data_model_location == data_model_location
        assert generator.component == component
        assert generator.output_path == expected_output_path

    def test_init_invalid_component(
        self, parsed_example_model, output_directory, example_data_model_path
    ):
        # GIVEN a component not present in the data model
        component = "InvalidComponent"

        # WHEN the JsonSchemaComponentGenerator class is initialized
        generator = JsonSchemaComponentGenerator(
            data_model_location=example_data_model_path,
            component=component,
            output_directory=output_directory.given,
            parsed_model=parsed_example_model,
        )

        # WHEN the component json schema is generated
        # THEN a ValueError should be raised
        with pytest.raises(ValueError):
            generator.get_component_json_schema()
