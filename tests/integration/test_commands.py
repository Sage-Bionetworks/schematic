"""Tests for CLI commands"""

import os
import uuid
from io import BytesIO

import pytest
import requests
from openpyxl import load_workbook
from click.testing import CliRunner

from schematic.configuration.configuration import Configuration
from schematic.manifest.commands import manifest
from schematic.models.commands import model

LIGHT_BLUE = "FFEAF7F9"  # Required cell
GRAY = "FFE0E0E0"  # Header cell
WHITE = "00000000"  # Optional cell


@pytest.fixture(name="runner")
def fixture_runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


class TestSubmitCommand:
    """Tests the schematic/models/commands submit command"""

    def test_submit_test_manifest(self, runner: CliRunner) -> None:
        """Tests for a successful submission"""
        # commented out, this command causes an error
        # https://sagebionetworks.jira.com/browse/SCHEMATIC-126
        """
        result = runner.invoke(
            model,
            [
                "--config",
                "config_example.yml",
                "submit",
                "-mp",
                "tests/data/mock_manifests/CLI_tests/CLI_biospecimen.csv",
                "-vc",
                "Biospecimen",
                "-mrt",
                "table_and_file",
                "-d",
                "syn23643250"
            ],
        )
        assert result.exit_code == 0
        """


class TestValidateCommand:
    """Tests the schematic/models/commands validate command"""

    def test_validate_valid_manifest(self, runner: CliRunner) -> None:
        """Tests for validation with no errors"""
        result = runner.invoke(
            model,
            [
                "--config",
                "config_example.yml",
                "validate",
                "--manifest_path",
                "tests/data/mock_manifests/Valid_Test_Manifest.csv",
                "--data_type",
                "MockComponent",
                "--project_scope",
                "syn54126707",
            ],
        )
        assert result.exit_code == 0
        assert result.output.split("\n")[4] == (
            "Your manifest has been validated successfully. "
            "There are no errors in your manifest, "
            "and it can be submitted without any modifications."
        )

    def test_validate_invalid_manifest(self, runner: CliRunner) -> None:
        """Tests for validation with no errors"""
        result = runner.invoke(
            model,
            [
                "--config",
                "config_example.yml",
                "validate",
                "--manifest_path",
                "tests/data/mock_manifests/CLI_tests/CLI_patient_invalid.csv",
                "--data_type",
                "Patient",
            ],
        )
        assert result.exit_code == 0
        assert result.output.split("\n")[3] == (
            "error: For attribute Family History in row 2 it does not appear "
            "as if you provided a comma delimited string. Please check your entry "
            "('Random'') and try again."
        )
        assert result.output.split("\n")[4].startswith("error: 'Random' is not one of")
        assert result.output.split("\n")[5].startswith("error: 'Random' is not one of")
        assert result.output.split("\n")[6].startswith("[['2', 'Family History',")


class TestManifestCommand:
    """Tests the schematic/manifest/commands validate manifest command"""

    def test_generate_empty_csv_manifest(self, runner: CliRunner) -> None:
        """Generate two empty csv manifests"""
        result = runner.invoke(manifest, ["--config", "config_example.yml", "get"])
        assert result.exit_code == 0
        # Assert the output has file creation messages
        assert result.output.split("\n")[7] == (
            "Find the manifest template using this CSV file path: "
            "tests/data/example.Biospecimen.manifest.csv"
        )
        assert result.output.split("\n")[10] == (
            "Find the manifest template using this CSV file path: "
            "tests/data/example.Patient.manifest.csv"
        )
        # Assert these files were created:
        assert os.path.isfile("tests/data/example.Biospecimen.manifest.csv")
        assert os.path.isfile("tests/data/example.Biospecimen.schema.json")
        assert os.path.isfile("tests/data/example.Patient.manifest.csv")
        assert os.path.isfile("tests/data/example.Patient.schema.json")
        # Remove created files:
        os.remove("tests/data/example.Biospecimen.manifest.csv")
        os.remove("tests/data/example.Biospecimen.schema.json")
        os.remove("tests/data/example.Patient.manifest.csv")
        os.remove("tests/data/example.Patient.schema.json")

    def test_generate_empty_google_sheet_manifests(self, runner: CliRunner) -> None:
        """Generate two empty google sheet manifests"""
        result = runner.invoke(
            manifest, ["--config", "config_example.yml", "get", "--sheet_url"]
        )
        assert result.exit_code == 0

        # Assert that generation of both manifest were successful based on message
        assert result.output.split("\n")[7] == (
            "Find the manifest template using this Google Sheet URL:"
        )
        assert result.output.split("\n")[8].startswith(
            "https://docs.google.com/spreadsheets/d/"
        )
        assert result.output.split("\n")[9] == (
            "Find the manifest template using this CSV file path: "
            "tests/data/example.Biospecimen.manifest.csv"
        )
        assert result.output.split("\n")[12] == (
            "Find the manifest template using this Google Sheet URL:"
        )
        assert result.output.split("\n")[13].startswith(
            "https://docs.google.com/spreadsheets/d/"
        )
        assert result.output.split("\n")[14] == (
            "Find the manifest template using this CSV file path: "
            "tests/data/example.Patient.manifest.csv"
        )

        # Assert these files were created:
        assert os.path.isfile("tests/data/example.Biospecimen.manifest.csv")
        assert os.path.isfile("tests/data/example.Biospecimen.schema.json")
        assert os.path.isfile("tests/data/example.Patient.manifest.csv")
        assert os.path.isfile("tests/data/example.Patient.schema.json")
        # Remove created files:
        os.remove("tests/data/example.Biospecimen.manifest.csv")
        os.remove("tests/data/example.Biospecimen.schema.json")
        os.remove("tests/data/example.Patient.manifest.csv")
        os.remove("tests/data/example.Patient.schema.json")

        # Get the google sheet urls form the message
        google_sheet_url1 = result.output.split("\n")[8]
        google_sheet_url2 = result.output.split("\n")[13]

        # Download the Google Sheets content as an Excel file and load into openpyxl
        export_url = f"{google_sheet_url1}/export?format=xlsx"
        response = requests.get(export_url)
        assert response.status_code == 200
        content = BytesIO(response.content)
        workbook = load_workbook(content)
        sheet1 = workbook["Sheet1"]

        # Track column positions
        columns = {cell.value: cell.column_letter for cell in sheet1[1]}

        assert sheet1[f"{columns['Sample ID']}1"].value == "Sample ID"
        assert sheet1[f"{columns['Sample ID']}2"].value is None

        assert sheet1[f"{columns['Patient ID']}1"].value == "Patient ID"
        assert sheet1[f"{columns['Patient ID']}2"].value is None

        assert sheet1[f"{columns['Tissue Status']}1"].value == "Tissue Status"
        assert sheet1[f"{columns['Tissue Status']}2"].value is None

        assert sheet1[f"{columns['Component']}1"].value == "Component"
        assert sheet1[f"{columns['Component']}2"].value == "Biospecimen"

        # AND there are no more columns in the first sheet
        assert sheet1[f"{columns['Component']}1"].offset(column=1).value is None

        # AND the first row is locked on scroll
        assert sheet1.freeze_panes == "A2"

        # AND each cell in the first row has a comment "TBD"
        for col in [
            "Sample ID",
            "Patient ID",
            "Tissue Status",
            "Component",
        ]:
            assert sheet1[f"{columns[col]}1"].comment.text == "TBD"

        # AND the dropdown lists exist and are as expected
        data_validations = sheet1.data_validations.dataValidation
        tissue_status_validation = None
        for dv in data_validations:
            if f"{columns['Tissue Status']}2" in dv.sqref:
                tissue_status_validation = dv
                continue
            # AND there are no other data validations
            assert False, f"Unexpected data validation found: {dv}"
        assert tissue_status_validation is not None
        assert tissue_status_validation.type == "list"
        assert tissue_status_validation.formula1 == "Sheet2!$C$2:$C$3"

        # AND the fill colors are as expected
        for col in ["Sample ID", "Patient ID", "Tissue Status", "Component"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == LIGHT_BLUE

        # Download the Google Sheets content as an Excel file and load into openpyxl
        export_url = f"{google_sheet_url2}/export?format=xlsx"
        response = requests.get(export_url)
        assert response.status_code == 200
        content = BytesIO(response.content)
        workbook = load_workbook(content)
        sheet1 = workbook["Sheet1"]

        # Track column positions
        columns = {cell.value: cell.column_letter for cell in sheet1[1]}

        # AND the content of the first sheet is as expected
        assert sheet1[f"{columns['Patient ID']}1"].value == "Patient ID"
        assert sheet1[f"{columns['Patient ID']}2"].value is None

        assert sheet1[f"{columns['Sex']}1"].value == "Sex"
        assert sheet1[f"{columns['Sex']}2"].value is None

        assert sheet1[f"{columns['Year of Birth']}1"].value == "Year of Birth"
        assert sheet1[f"{columns['Year of Birth']}2"].value is None

        assert sheet1[f"{columns['Diagnosis']}1"].value == "Diagnosis"
        assert sheet1[f"{columns['Diagnosis']}2"].value is None

        assert sheet1[f"{columns['Component']}1"].value == "Component"
        assert sheet1[f"{columns['Component']}2"].value == "Patient"

        assert sheet1[f"{columns['Cancer Type']}1"].value == "Cancer Type"
        assert sheet1[f"{columns['Cancer Type']}2"].value is None

        assert sheet1[f"{columns['Family History']}1"].value == "Family History"
        assert sheet1[f"{columns['Family History']}2"].value is None

        # AND there are no more columns in the first sheet
        assert sheet1[f"{columns['Family History']}1"].offset(column=1).value is None

        # AND the first row is locked on scroll
        assert sheet1.freeze_panes == "A2"

        # AND each cell in the first row has a comment "TBD"
        for col in [
            "Patient ID",
            "Sex",
            "Year of Birth",
            "Diagnosis",
            "Component",
            "Cancer Type",
            "Family History",
        ]:
            assert sheet1[f"{columns[col]}1"].comment.text == "TBD"

        # AND the comment in "Family History" cell is as expected
        assert (
            sheet1[f"{columns['Family History']}2"].comment.text
            == "Please enter applicable comma-separated items selected from the set of allowable terms for this attribute. See our data standards for allowable terms"
        )

        # AND the dropdown lists exist and are as expected
        data_validations = sheet1.data_validations.dataValidation
        sex_validation = None
        diagnosis_validation = None
        cancer_type_validation = None
        for dv in data_validations:
            if f"{columns['Sex']}2" in dv.sqref:
                sex_validation = dv
                continue
            elif f"{columns['Diagnosis']}2" in dv.sqref:
                diagnosis_validation = dv
                continue
            elif f"{columns['Cancer Type']}2" in dv.sqref:
                cancer_type_validation = dv
                continue
            # AND there are no other data validations
            assert False, f"Unexpected data validation found: {dv}"

        assert sex_validation is not None
        assert sex_validation.type == "list"
        assert sex_validation.formula1 == "Sheet2!$B$2:$B$4"

        assert diagnosis_validation is not None
        assert diagnosis_validation.type == "list"
        assert diagnosis_validation.formula1 == "Sheet2!$D$2:$D$3"

        assert cancer_type_validation is not None
        assert cancer_type_validation.type == "list"
        assert cancer_type_validation.formula1 == "Sheet2!$F$2:$F$6"

        # AND the fill colors are as expected
        for col in ["Patient ID", "Sex", "Diagnosis", "Component"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == LIGHT_BLUE

        for col in ["Year of Birth", "Cancer Type", "Family History"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == GRAY

        for col in ["Patient ID", "Sex", "Diagnosis", "Component"]:
            assert sheet1[f"{columns[col]}2"].fill.start_color.index == LIGHT_BLUE

        for col in ["Year of Birth", "Cancer Type", "Family History"]:
            assert sheet1[f"{columns[col]}2"].fill.start_color.index == WHITE

    def test_generate_empty_excel_manifest(self, runner: CliRunner) -> None:
        """Generate an empty patient excel manifest"""
        result = runner.invoke(
            manifest,
            ["--config", "config_example.yml", "get", "--output_xlsx", "./test.xlsx"],
        )
        assert result.exit_code == 0
        assert (
            result.output.split("\n")[7]
            == "Find the manifest template using this Excel file path: ./test.xlsx"
        )

        # Assert these files were created:
        assert os.path.isfile("tests/data/example.Biospecimen.schema.json")
        assert os.path.isfile("tests/data/example.Patient.schema.json")
        assert os.path.isfile("test.xlsx")

        workbook = load_workbook("test.xlsx")

        # Remove created files:
        os.remove("tests/data/example.Biospecimen.schema.json")
        os.remove("tests/data/example.Patient.schema.json")
        os.remove("test.xlsx")

        sheet1 = workbook["Sheet1"]
        # Track column positions
        columns = {cell.value: cell.column_letter for cell in sheet1[1]}

        # AND the content of the first sheet is as expected
        assert sheet1[f"{columns['Patient ID']}1"].value == "Patient ID"
        assert sheet1[f"{columns['Patient ID']}2"].value is None

        assert sheet1[f"{columns['Sex']}1"].value == "Sex"
        assert sheet1[f"{columns['Sex']}2"].value is None

        assert sheet1[f"{columns['Year of Birth']}1"].value == "Year of Birth"
        assert sheet1[f"{columns['Year of Birth']}2"].value is None

        assert sheet1[f"{columns['Diagnosis']}1"].value == "Diagnosis"
        assert sheet1[f"{columns['Diagnosis']}2"].value is None

        assert sheet1[f"{columns['Component']}1"].value == "Component"
        assert sheet1[f"{columns['Component']}2"].value == "Patient"

        assert sheet1[f"{columns['Cancer Type']}1"].value == "Cancer Type"
        assert sheet1[f"{columns['Cancer Type']}2"].value is None

        assert sheet1[f"{columns['Family History']}1"].value == "Family History"
        assert sheet1[f"{columns['Family History']}2"].value is None

        # AND there are no more columns in the first sheet
        assert sheet1[f"{columns['Family History']}1"].offset(column=1).value is None

        # AND the first row is locked on scroll
        assert sheet1.freeze_panes == "A2"

        # AND each cell in the first row has a comment "TBD"
        for col in [
            "Patient ID",
            "Sex",
            "Year of Birth",
            "Diagnosis",
            "Component",
            "Cancer Type",
            "Family History",
        ]:
            assert sheet1[f"{columns[col]}1"].comment.text == "TBD"

        # AND the comment in "Family History" cell is as expected
        assert (
            sheet1[f"{columns['Family History']}2"].comment.text
            == "Please enter applicable comma-separated items selected from the set of allowable terms for this attribute. See our data standards for allowable terms"
        )

        # AND the dropdown lists exist and are as expected
        data_validations = sheet1.data_validations.dataValidation
        sex_validation = None
        diagnosis_validation = None
        cancer_type_validation = None
        for dv in data_validations:
            if f"{columns['Sex']}2" in dv.sqref:
                sex_validation = dv
                continue
            elif f"{columns['Diagnosis']}2" in dv.sqref:
                diagnosis_validation = dv
                continue
            elif f"{columns['Cancer Type']}2" in dv.sqref:
                cancer_type_validation = dv
                continue
            # AND there are no other data validations
            assert False, f"Unexpected data validation found: {dv}"

        assert sex_validation is not None
        assert sex_validation.type == "list"
        assert sex_validation.formula1 == "Sheet2!$B$2:$B$4"

        assert diagnosis_validation is not None
        assert diagnosis_validation.type == "list"
        assert diagnosis_validation.formula1 == "Sheet2!$D$2:$D$3"

        assert cancer_type_validation is not None
        assert cancer_type_validation.type == "list"
        assert cancer_type_validation.formula1 == "Sheet2!$F$2:$F$6"

        # AND the fill colors are as expected
        for col in ["Patient ID", "Sex", "Diagnosis", "Component"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == LIGHT_BLUE

        for col in ["Year of Birth", "Cancer Type", "Family History"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == GRAY

        for col in ["Patient ID", "Sex", "Diagnosis", "Component"]:
            assert sheet1[f"{columns[col]}2"].fill.start_color.index == LIGHT_BLUE

        for col in ["Year of Birth", "Cancer Type", "Family History"]:
            assert sheet1[f"{columns[col]}2"].fill.start_color.index == WHITE

    def test_generate_bulk_rna_google_sheet_manifest(self, runner: CliRunner) -> None:
        """Generate bulk_rna google sheet manifest"""
        result = runner.invoke(
            manifest,
            [
                "--config",
                "tests/data/test_configs/CLI_test_config.yml",
                "get",
                "--dataset_id",
                "syn63923432",
                "--data_type",
                "BulkRNA-seqAssay",
                "--sheet_url",
            ],
        )
        assert result.exit_code == 0
        assert result.output.split("\n")[7] == (
            "Find the manifest template using this Google Sheet URL:"
        )
        assert result.output.split("\n")[8].startswith(
            "https://docs.google.com/spreadsheets/d/"
        )
        assert result.output.split("\n")[9] == (
            "Find the manifest template using this CSV file path: "
            "tests/data/example.BulkRNA-seqAssay.manifest.csv"
        )
        # Assert these files were created:
        assert os.path.isfile("tests/data/example.BulkRNA-seqAssay.schema.json")
        assert os.path.isfile("tests/data/example.BulkRNA-seqAssay.manifest.csv")
        # Remove created files:
        os.remove("tests/data/example.BulkRNA-seqAssay.schema.json")
        os.remove("tests/data/example.BulkRNA-seqAssay.manifest.csv")

        google_sheet_url = result.output.split("\n")[8]

        # Download the Google Sheets content as an Excel file and load into openpyxl
        export_url = f"{google_sheet_url}/export?format=xlsx"
        response = requests.get(export_url)
        assert response.status_code == 200
        content = BytesIO(response.content)
        workbook = load_workbook(content)
        sheet1 = workbook["Sheet1"]
        sheet2 = workbook["Sheet2"]

        # Track column positions
        columns = {cell.value: cell.column_letter for cell in sheet1[1]}

        # AND the content of the first sheet is as expected
        assert columns["Filename"] is not None
        assert columns["Sample ID"] is not None
        assert columns["File Format"] is not None
        assert columns["Component"] is not None
        assert columns["Genome Build"] is not None
        assert columns["Genome FASTA"] is not None
        assert columns["entityId"] is not None

        assert sheet1[f"{columns['Sample ID']}2"].value == 2022
        assert sheet1[f"{columns['Sample ID']}3"].value is None
        assert sheet1[f"{columns['Sample ID']}4"].value is None
        assert sheet1[f"{columns['Sample ID']}5"].value is None
        assert sheet1[f"{columns['File Format']}2"].value == "CSV/TSV"
        assert sheet1[f"{columns['File Format']}3"].value is None
        assert sheet1[f"{columns['File Format']}4"].value is None
        assert sheet1[f"{columns['File Format']}5"].value is None
        assert sheet1[f"{columns['Component']}2"].value == "BulkRNA-seqAssay"
        assert sheet1[f"{columns['Component']}3"].value is None
        assert sheet1[f"{columns['Component']}4"].value is None
        assert sheet1[f"{columns['Component']}5"].value is None
        assert sheet1[f"{columns['Genome Build']}2"].value == "GRCm38"
        assert sheet1[f"{columns['Genome Build']}3"].value is None
        assert sheet1[f"{columns['Genome Build']}4"].value is None
        assert sheet1[f"{columns['Genome Build']}5"].value is None
        assert sheet1[f"{columns['Genome FASTA']}2"].value is None
        assert sheet1[f"{columns['Genome FASTA']}3"].value is None
        assert sheet1[f"{columns['Genome FASTA']}4"].value is None
        assert sheet1[f"{columns['Genome FASTA']}5"].value is None
        assert sheet1[f"{columns['entityId']}2"].value == "syn28278954"
        assert sheet1[f"{columns['entityId']}3"].value == "syn63923439"
        assert sheet1[f"{columns['entityId']}4"].value == "syn63923441"
        assert sheet1[f"{columns['entityId']}5"].value == "syn63923444"

        # AND there are no more columns in the first sheet
        assert sheet1[f"{columns['entityId']}1"].offset(column=1).value is None

        # AND the first row is locked on scroll
        assert sheet1.freeze_panes == "A2"

        # AND each of these cells in the first row has a comment "TBD"
        for col in [
            "Filename",
            "Sample ID",
            "File Format",
            "Component",
            "Genome Build",
            "Genome FASTA",
        ]:
            assert sheet1[f"{columns[col]}1"].comment.text == "TBD"

        # AND each of these cells in the first row do not have a comment
        for col in [
            "entityId",
        ]:
            assert sheet1[f"{columns[col]}1"].comment is None

        # AND the dropdown lists exist and are as expected
        data_validations = sheet1.data_validations.dataValidation
        file_format_validation = None
        genome_build_validation = None
        for dv in data_validations:
            if f"{columns['File Format']}2" in dv.sqref:
                file_format_validation = dv
                continue
            elif f"{columns['Genome Build']}2" in dv.sqref:
                genome_build_validation = dv
                continue
            # AND there are no other data validations
            assert False, f"Unexpected data validation found: {dv}"

        assert file_format_validation is not None
        assert file_format_validation.type == "list"
        assert (
            file_format_validation.formula1
            == f"Sheet2!${columns['File Format']}$2:${columns['File Format']}$5"
        )

        assert genome_build_validation is not None
        assert genome_build_validation.type == "list"
        assert (
            genome_build_validation.formula1
            == f"Sheet2!${columns['Genome Build']}$2:${columns['Genome Build']}$5"
        )

        # AND the fill colors are as expected
        for col in ["Filename", "Sample ID", "File Format", "Component"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == LIGHT_BLUE

        for col in [
            "Genome Build",
            "Genome FASTA",
            "entityId",
        ]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == GRAY

        # AND conditional formatting is functioning as expected (MANUAL VERIFICATION)
        workbook["Sheet1"][f"{columns['File Format']}2"].value = "BAM"
        workbook["Sheet1"][f"{columns['File Format']}3"].value = "CRAM"
        workbook["Sheet1"][f"{columns['File Format']}4"].value = "FASTQ"

        # AND the workbook contains two sheets: "Sheet1" and "Sheet2"
        assert workbook.sheetnames == ["Sheet1", "Sheet2"]

        # AND the second sheet is hidden
        assert sheet2.sheet_state == "hidden"

        # AND the values in "Sheet2" are as expected
        assert sheet2["A1"].value == "Filename"
        assert sheet2["B1"].value == "Sample ID"
        assert sheet2["C1"].value == "File Format"
        assert sheet2["D1"].value == "Component"
        assert sheet2["E1"].value == "Genome Build"
        assert sheet2["F1"].value == "Genome FASTA"

        assert sheet2["A2"].value is None
        assert sheet2["B2"].value is None
        assert sheet2["C2"].value == "BAM"
        assert sheet2["D2"].value is None
        assert sheet2["E2"].value == "GRCh37"
        assert sheet2["F2"].value is None

        assert sheet2["A3"].value is None
        assert sheet2["B3"].value is None
        assert sheet2["C3"].value == "CRAM"
        assert sheet2["D3"].value is None
        assert sheet2["E3"].value == "GRCh38"
        assert sheet2["F3"].value is None

        assert sheet2["A4"].value is None
        assert sheet2["B4"].value is None
        assert sheet2["C4"].value == "CSV/TSV"
        assert sheet2["D4"].value is None
        assert sheet2["E4"].value == "GRCm38"
        assert sheet2["F4"].value is None

        assert sheet2["A5"].value is None
        assert sheet2["B5"].value is None
        assert sheet2["C5"].value == "FASTQ"
        assert sheet2["D5"].value is None
        assert sheet2["E5"].value == "GRCm39"
        assert sheet2["F5"].value is None

        # AND there are no more columns in the second sheet
        assert sheet2["G1"].value is None

    def test_generate_bulk_rna_google_sheet_manifest_with_annotations(
        self, runner: CliRunner
    ) -> None:
        """Generate bulk_rna google sheet manifest"""
        result = runner.invoke(
            manifest,
            [
                "--config",
                "config_example.yml",
                "get",
                "--dataset_id",
                "syn25614635",
                "--data_type",
                "BulkRNA-seqAssay",
                "--sheet_url",
                "--use_annotations",
            ],
        )
        assert result.exit_code == 0
        assert result.output.split("\n")[10] == (
            "Find the manifest template using this Google Sheet URL:"
        )
        assert result.output.split("\n")[11].startswith(
            "https://docs.google.com/spreadsheets/d/"
        )
        assert result.output.split("\n")[12] == (
            "Find the manifest template using this CSV file path: "
            "tests/data/example.BulkRNA-seqAssay.manifest.csv"
        )

        # Assert these files were created:
        assert os.path.isfile("tests/data/example.BulkRNA-seqAssay.schema.json")
        assert os.path.isfile("tests/data/example.BulkRNA-seqAssay.manifest.csv")
        # Remove created files:
        os.remove("tests/data/example.BulkRNA-seqAssay.schema.json")
        os.remove("tests/data/example.BulkRNA-seqAssay.manifest.csv")

        google_sheet_url = result.output.split("\n")[11]

        # Download the Google Sheets content as an Excel file and load into openpyxl
        export_url = f"{google_sheet_url}/export?format=xlsx"
        response = requests.get(export_url)
        assert response.status_code == 200
        content = BytesIO(response.content)
        workbook = load_workbook(content)
        sheet1 = workbook["Sheet1"]
        sheet2 = workbook["Sheet2"]

        # Track column positions
        columns = {cell.value: cell.column_letter for cell in sheet1[1]}

        # AND the content of the first sheet is as expected
        assert columns["Filename"] is not None
        assert columns["Sample ID"] is not None
        assert columns["File Format"] is not None
        assert columns["Component"] is not None
        assert columns["Genome Build"] is not None
        assert columns["Genome FASTA"] is not None
        assert columns["impact"] is not None
        assert columns["author"] is not None
        assert columns["eTag"] is not None
        assert columns["IsImportantText"] is not None
        assert columns["IsImportantBool"] is not None
        assert columns["confidence"] is not None
        assert columns["date"] is not None
        assert columns["Year of Birth"] is not None
        assert columns["entityId"] is not None

        assert sheet1[f"{columns['Filename']}2"].value is not None
        assert sheet1[f"{columns['Sample ID']}2"].value is None
        assert sheet1[f"{columns['File Format']}2"].value is not None
        assert sheet1[f"{columns['Component']}2"].value is not None
        assert sheet1[f"{columns['Genome Build']}2"].value is None
        assert sheet1[f"{columns['Genome FASTA']}2"].value is None
        assert sheet1[f"{columns['impact']}2"].value is not None
        assert sheet1[f"{columns['author']}2"].value is not None
        assert sheet1[f"{columns['eTag']}2"].value is not None
        assert sheet1[f"{columns['IsImportantText']}2"].value is not None
        assert sheet1[f"{columns['IsImportantBool']}2"].value is not None
        assert sheet1[f"{columns['confidence']}2"].value is not None
        assert sheet1[f"{columns['date']}2"].value is None
        assert sheet1[f"{columns['Year of Birth']}2"].value is not None
        assert sheet1[f"{columns['entityId']}2"].value is not None

        assert sheet1[f"{columns['Filename']}3"].value is not None
        assert sheet1[f"{columns['Sample ID']}3"].value is None
        assert sheet1[f"{columns['File Format']}3"].value is not None
        assert sheet1[f"{columns['Component']}3"].value is not None
        assert sheet1[f"{columns['Genome Build']}3"].value is None
        assert sheet1[f"{columns['Genome FASTA']}3"].value is None
        assert sheet1[f"{columns['impact']}3"].value is None
        assert sheet1[f"{columns['author']}3"].value is None
        assert sheet1[f"{columns['eTag']}3"].value is not None
        assert sheet1[f"{columns['IsImportantText']}3"].value is None
        assert sheet1[f"{columns['IsImportantBool']}3"].value is None
        assert sheet1[f"{columns['confidence']}3"].value is not None
        assert sheet1[f"{columns['date']}3"].value is not None
        assert sheet1[f"{columns['Year of Birth']}3"].value is None
        assert sheet1[f"{columns['entityId']}3"].value is not None

        assert sheet1[f"{columns['Filename']}4"].value is not None
        assert sheet1[f"{columns['Sample ID']}4"].value is None
        assert sheet1[f"{columns['File Format']}4"].value is not None
        assert sheet1[f"{columns['Component']}4"].value is not None
        assert sheet1[f"{columns['Genome Build']}4"].value is None
        assert sheet1[f"{columns['Genome FASTA']}4"].value is None
        assert sheet1[f"{columns['impact']}4"].value is None
        assert sheet1[f"{columns['author']}4"].value is None
        assert sheet1[f"{columns['eTag']}4"].value is not None
        assert sheet1[f"{columns['IsImportantText']}4"].value is not None
        assert sheet1[f"{columns['IsImportantBool']}4"].value is not None
        assert sheet1[f"{columns['confidence']}4"].value is None
        assert sheet1[f"{columns['date']}4"].value is None
        assert sheet1[f"{columns['Year of Birth']}4"].value is None
        assert sheet1[f"{columns['entityId']}4"].value is not None

        # AND there are no more columns in the first sheet
        assert sheet1[f"{columns['entityId']}1"].offset(column=1).value is None

        # AND the first row is locked on scroll
        assert sheet1.freeze_panes == "A2"

        # AND each of these cells in the first row has a comment "TBD"
        for col in [
            "Filename",
            "Sample ID",
            "File Format",
            "Component",
            "Genome Build",
            "Genome FASTA",
        ]:
            assert sheet1[f"{columns[col]}1"].comment.text == "TBD"

        # AND each of these cells in the first row do not have a comment
        for col in [
            "impact",
            "author",
            "eTag",
            "IsImportantText",
            "IsImportantBool",
            "confidence",
            "date",
            "Year of Birth",
            "entityId",
        ]:
            assert sheet1[f"{columns[col]}1"].comment is None

        # AND the dropdown lists exist and are as expected
        data_validations = sheet1.data_validations.dataValidation
        file_format_validation = None
        genome_build_validation = None
        for dv in data_validations:
            if f"{columns['File Format']}2" in dv.sqref:
                file_format_validation = dv
                continue
            elif f"{columns['Genome Build']}2" in dv.sqref:
                genome_build_validation = dv
                continue
            # AND there are no other data validations
            assert False, f"Unexpected data validation found: {dv}"

        assert file_format_validation is not None
        assert file_format_validation.type == "list"
        assert (
            file_format_validation.formula1
            == f"Sheet2!${columns['File Format']}$2:${columns['File Format']}$5"
        )

        assert genome_build_validation is not None
        assert genome_build_validation.type == "list"
        assert (
            genome_build_validation.formula1
            == f"Sheet2!${columns['Genome Build']}$2:${columns['Genome Build']}$5"
        )

        # AND the fill colors are as expected
        for col in ["Filename", "Sample ID", "File Format", "Component"]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == LIGHT_BLUE

        for col in [
            "Genome Build",
            "Genome FASTA",
            "impact",
            "author",
            "eTag",
            "IsImportantText",
            "IsImportantBool",
            "confidence",
            "date",
            "Year of Birth",
            "entityId",
        ]:
            assert sheet1[f"{columns[col]}1"].fill.start_color.index == GRAY

        # AND conditional formatting is functioning as expected (MANUAL VERIFICATION)
        workbook["Sheet1"][f"{columns['File Format']}2"].value = "BAM"
        workbook["Sheet1"][f"{columns['File Format']}3"].value = "CRAM"
        workbook["Sheet1"][f"{columns['File Format']}4"].value = "FASTQ"

        # AND the workbook contains two sheets: "Sheet1" and "Sheet2"
        assert workbook.sheetnames == ["Sheet1", "Sheet2"]

        # AND the second sheet is hidden
        assert sheet2.sheet_state == "hidden"

        # AND the values in "Sheet2" are as expected
        assert sheet2["A1"].value == "Filename"
        assert sheet2["B1"].value == "Sample ID"
        assert sheet2["C1"].value == "File Format"
        assert sheet2["D1"].value == "Component"
        assert sheet2["E1"].value == "Genome Build"
        assert sheet2["F1"].value == "Genome FASTA"

        assert sheet2["A2"].value is None
        assert sheet2["B2"].value is None
        assert sheet2["C2"].value == "BAM"
        assert sheet2["D2"].value is None
        assert sheet2["E2"].value == "GRCh37"
        assert sheet2["F2"].value is None

        assert sheet2["A3"].value is None
        assert sheet2["B3"].value is None
        assert sheet2["C3"].value == "CRAM"
        assert sheet2["D3"].value is None
        assert sheet2["E3"].value == "GRCh38"
        assert sheet2["F3"].value is None

        assert sheet2["A4"].value is None
        assert sheet2["B4"].value is None
        assert sheet2["C4"].value == "CSV/TSV"
        assert sheet2["D4"].value is None
        assert sheet2["E4"].value == "GRCm38"
        assert sheet2["F4"].value is None

        assert sheet2["A5"].value is None
        assert sheet2["B5"].value is None
        assert sheet2["C5"].value == "FASTQ"
        assert sheet2["D5"].value is None
        assert sheet2["E5"].value == "GRCm39"
        assert sheet2["F5"].value is None

        # AND there are no more columns in the second sheet
        assert sheet2["G1"].value is None

    def test_generate_mock_component_excel_manifest(self, runner: CliRunner) -> None:
        """Generate an excel manifest with a MockComponent"""
        result = runner.invoke(
            manifest,
            [
                "--config",
                "tests/data/test_configs/CLI_test_config2.yml",
                "get",
                "--output_xlsx",
                "test-example.xlsx",
                "--dataset_id",
                "syn52746566",
            ],
        )
        assert result.exit_code == 0
        assert result.output.split("\n")[8] == (
            "Find the manifest template using this Excel file path: test-example.xlsx"
        )

        # Assert these files were created:
        assert os.path.isfile("tests/data/example.MockComponent.schema.json")
        assert os.path.isfile("test-example.xlsx")

        workbook = load_workbook("test-example.xlsx")

        # Remove created files:
        os.remove("tests/data/example.MockComponent.schema.json")
        os.remove("test-example.xlsx")

        sheet1 = workbook["Sheet1"]
        # Track column positions
        columns = {cell.value: cell.column_letter for cell in sheet1[1]}

        # AND the content of the first sheet is as expected
        assert sheet1[f"{columns['Component']}1"].value == "Component"
        assert sheet1[f"{columns['Component']}2"].value == "MockComponent"
        assert sheet1[f"{columns['Component']}3"].value == "MockComponent"

        assert sheet1[f"{columns['Check List']}1"].value == "Check List"
        assert sheet1[f"{columns['Check List']}2"].value is not None
        assert sheet1[f"{columns['Check List']}3"].value is not None

        assert sheet1[f"{columns['Check Regex List']}1"].value == "Check Regex List"
        assert sheet1[f"{columns['Check Regex List']}2"].value is not None
        assert sheet1[f"{columns['Check Regex List']}3"].value is not None

        assert sheet1[f"{columns['Check Regex Single']}1"].value == "Check Regex Single"
        assert sheet1[f"{columns['Check Regex Single']}2"].value is not None
        assert sheet1[f"{columns['Check Regex Single']}3"].value is not None

        assert sheet1[f"{columns['Check Regex Format']}1"].value == "Check Regex Format"
        assert sheet1[f"{columns['Check Regex Format']}2"].value is not None
        assert sheet1[f"{columns['Check Regex Format']}3"].value is not None

        assert (
            sheet1[f"{columns['Check Regex Integer']}1"].value == "Check Regex Integer"
        )
        assert sheet1[f"{columns['Check Regex Integer']}2"].value is not None
        assert sheet1[f"{columns['Check Regex Integer']}3"].value is not None

        assert sheet1[f"{columns['Check Num']}1"].value == "Check Num"
        assert sheet1[f"{columns['Check Num']}2"].value is not None
        assert sheet1[f"{columns['Check Num']}3"].value is not None

        assert sheet1[f"{columns['Check Float']}1"].value == "Check Float"
        assert sheet1[f"{columns['Check Float']}2"].value is not None
        assert sheet1[f"{columns['Check Float']}3"].value is not None

        assert sheet1[f"{columns['Check Int']}1"].value == "Check Int"
        assert sheet1[f"{columns['Check Int']}2"].value is not None
        assert sheet1[f"{columns['Check Int']}3"].value is not None

        assert sheet1[f"{columns['Check String']}1"].value == "Check String"
        assert sheet1[f"{columns['Check String']}2"].value is not None
        assert sheet1[f"{columns['Check String']}3"].value is not None


class TestDownloadManifest:
    """Tests the command line interface for downloading a manifest"""

    def test_download_manifest_found(
        self,
        runner: CliRunner,
        config: Configuration,
    ) -> None:
        # GIVEN a manifest name to download as
        manifest_name = f"{uuid.uuid4()}"

        # AND a dataset id
        dataset_id = "syn23643250"

        # AND a configuration file
        config.load_config("config_example.yml")

        # WHEN the download command is run
        result = runner.invoke(
            cli=manifest,
            args=[
                "--config",
                config.config_path,
                "download",
                "--new_manifest_name",
                manifest_name,
                "--dataset_id",
                dataset_id,
            ],
        )

        # THEN the command should run successfully
        assert result.exit_code == 0

        # AND the manifest file should be created
        expected_manifest_file = os.path.join(
            config.manifest_folder, f"{manifest_name}.csv"
        )
        assert os.path.exists(expected_manifest_file)
        try:
            os.remove(expected_manifest_file)
        except Exception:
            pass

    def test_download_manifest_not_found(
        self,
        runner: CliRunner,
        config: Configuration,
    ) -> None:
        # GIVEN a manifest name to download as
        manifest_name = f"{uuid.uuid4()}"

        # AND a dataset id that does not exist
        dataset_id = "syn1234"

        # AND a configuration file
        config.load_config("config_example.yml")

        # WHEN the download command is run
        result = runner.invoke(
            cli=manifest,
            args=[
                "--config",
                config.config_path,
                "download",
                "--new_manifest_name",
                manifest_name,
                "--dataset_id",
                dataset_id,
            ],
        )

        # THEN the command should not run successfully
        assert result.exit_code == 1

        # AND the manifest file should not be created
        expected_manifest_file = os.path.join(
            config.manifest_folder, f"{manifest_name}.csv"
        )
        assert not os.path.exists(expected_manifest_file)
