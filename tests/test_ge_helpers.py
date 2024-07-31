from typing import Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest

from schematic.models.GE_Helpers import GreatExpectationsHelpers
from tests.conftest import Helpers


@pytest.fixture(scope="class")
def mock_ge_helpers(
    helpers: Helpers,
) -> Generator[GreatExpectationsHelpers, None, None]:
    """Fixture for creating a GreatExpectationsHelpers object"""
    dmge = helpers.get_data_model_graph_explorer(path="example.model.jsonld")
    unimplemented_expectations = ["url"]
    test_manifest_path = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
    manifest = helpers.get_data_frame(test_manifest_path)

    ge_helpers = GreatExpectationsHelpers(
        dmge=dmge,
        unimplemented_expectations=unimplemented_expectations,
        manifest=manifest,
        manifestPath=test_manifest_path,
    )
    yield ge_helpers


class TestGreatExpectationsHelpers:
    def test_add_expectation_suite_if_not_exists_does_not_exist(
        self, mock_ge_helpers: Generator[GreatExpectationsHelpers, None, None]
    ) -> None:
        """test add_expectation_suite_if_not_exists method when the expectation suite does not exists"""
        # mock context provided by ge_helpers
        mock_ge_helpers.context = MagicMock()
        mock_ge_helpers.context.list_expectation_suite_names.return_value = []

        # Call the method
        result = mock_ge_helpers.add_expectation_suite_if_not_exists()

        # Make sure the method of creating expectation suites if it doesn't exist
        mock_ge_helpers.context.list_expectation_suite_names.assert_called_once()
        mock_ge_helpers.context.add_expectation_suite.assert_called_once_with(
            expectation_suite_name="Manifest_test_suite"
        )

    def test_add_expectation_suite_if_not_exists_does_exist(
        self, mock_ge_helpers: Generator[GreatExpectationsHelpers, None, None]
    ) -> None:
        """test add_expectation_suite_if_not_exists method when the expectation suite does exists"""
        # mock context provided by ge_helpers
        mock_ge_helpers.context = MagicMock()
        mock_ge_helpers.context.list_expectation_suite_names.return_value = [
            "Manifest_test_suite"
        ]
        mock_ge_helpers.context.list_checkpoints.return_value = ["test_checkpoint"]

        # Call the method
        result = mock_ge_helpers.add_expectation_suite_if_not_exists()

        # Make sure the method of deleting suites gets called
        mock_ge_helpers.context.list_expectation_suite_names.assert_called_once()
        mock_ge_helpers.context.delete_expectation_suite.assert_called_once_with(
            "Manifest_test_suite"
        )
        mock_ge_helpers.context.add_expectation_suite.assert_called_once_with(
            expectation_suite_name="Manifest_test_suite"
        )
        mock_ge_helpers.context.delete_checkpoint.assert_called_once_with(
            "test_checkpoint"
        )
