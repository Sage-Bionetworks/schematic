"""Unit tests for DataModelGraphExplorer"""

import pytest
from schematic.schemas.data_model_graph import DataModelGraphExplorer


@pytest.mark.parametrize(
    "node_label, relationship, expected_nodes",
    [
        # rangeValue will get an attributes valid values
        (
            "CancerType",
            "rangeValue",
            ["Breast", "Colorectal", "Lung", "Prostate", "Skin"],
        ),
        (
            "FamilyHistory",
            "rangeValue",
            ["Breast", "Colorectal", "Lung", "Prostate", "Skin"],
        ),
        ("FileFormat", "rangeValue", ["BAM", "CRAM", "CSV/TSV", "FASTQ"]),
        # requiresDependency will get an components attributes
        (
            "Patient",
            "requiresDependency",
            ["Component", "Diagnosis", "PatientID", "Sex", "YearofBirth"],
        ),
        (
            "Biospecimen",
            "requiresDependency",
            ["Component", "PatientID", "SampleID", "TissueStatus"],
        ),
        # requiresDependency will get an attributes dependencies
        ("Cancer", "requiresDependency", ["CancerType", "FamilyHistory"]),
    ],
)
def test_get_adjacent_nodes_by_relationship(
    dmge: DataModelGraphExplorer,
    node_label: str,
    relationship: str,
    expected_nodes: list[str],
) -> None:
    assert (
        sorted(dmge.get_adjacent_nodes_by_relationship(node_label, relationship))
        == expected_nodes
    )
