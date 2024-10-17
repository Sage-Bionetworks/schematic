import pandas as pd

from schematic.models.validate_attribute import ValidateAttribute
from schematic.schemas.data_model_graph import DataModelGraphExplorer

CHECK_URL_NODE_NAME = "Check URL"
VALIDATION_RULE_URL = "url"


class TestValidateAttribute:
    """Integration tests for the ValidateAttribute class."""

    def test_url_validation_valid_url(self, dmge: DataModelGraphExplorer) -> None:
        # GIVEN a valid URL:
        url = "https://github.com/Sage-Bionetworks/schematic"

        # AND a pd.core.series.Series that contains this URL
        content = pd.Series(data=[url], name=CHECK_URL_NODE_NAME)

        # AND a validation attribute
        validator = ValidateAttribute(dmge=dmge)

        # WHEN the URL is validated
        result = validator.url_validation(
            val_rule=VALIDATION_RULE_URL, manifest_col=content
        )

        # THEN the result should pass validation
        assert result == ([], [])

    def test_url_validation_valid_doi(self, dmge: DataModelGraphExplorer) -> None:
        # GIVEN a valid URL:
        url = "https://doi.org/10.1158/0008-5472.can-23-0128"

        # AND a pd.core.series.Series that contains this URL
        content = pd.Series(data=[url], name=CHECK_URL_NODE_NAME)

        # AND a validation attribute
        validator = ValidateAttribute(dmge=dmge)

        # WHEN the URL is validated
        result = validator.url_validation(
            val_rule=VALIDATION_RULE_URL, manifest_col=content
        )

        # THEN the result should pass validation
        assert result == ([], [])

    def test_url_validation_invalid_url(self, dmge: DataModelGraphExplorer) -> None:
        # GIVEN an invalid URL:
        url = "http://googlef.com/"

        # AND a pd.core.series.Series that contains this URL
        content = pd.Series(data=[url], name=CHECK_URL_NODE_NAME)

        # AND a validation attribute
        validator = ValidateAttribute(dmge=dmge)

        # WHEN the URL is validated
        result = validator.url_validation(
            val_rule=VALIDATION_RULE_URL, manifest_col=content
        )

        # THEN the result should not pass validation
        assert result == (
            [
                [
                    "2",
                    "Check URL",
                    "For the attribute 'Check URL', on row 2, the URL provided (http://googlef.com/) does not conform to the standards of a URL. Please make sure you are entering a real, working URL as required by the Schema.",
                    "http://googlef.com/",
                ]
            ],
            [],
        )

    # See slack discussion, to turn test back on at a later time: https://sagebionetworks.jira.com/browse/FDS-2509
    # def test__get_target_manifest_dataframes(
    #     self, dmge: DataModelGraphExplorer
    # ) -> None:
    #     """
    #     This test checks that the method successfully returns manifests from Synapse

    #     """
    #     validator = ValidateAttribute(dmge=dmge)
    #     manifests = validator._get_target_manifest_dataframes(  # pylint:disable= protected-access
    #         "patient", project_scope=["syn54126707"]
    #     )
    #     assert list(manifests.keys()) == ["syn54126997", "syn54127001"]
