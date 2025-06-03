"""Unit tests for DataModelRelationships"""

import pytest
from schematic.schemas.data_model_relationships import DataModelRelationships


class TestDataModelRelationships:
    """Tests for DataModelRelationships class"""

    def test_get_allowed_values(self, dmr: DataModelRelationships) -> None:
        """Tests for DataModelRelationships.get_allowed_values"""
        result = dmr.get_allowed_values("columnType")
        assert result == ["string", "integer", "number", "boolean"]

    def test_get_allowed_values_value_error(self, dmr: DataModelRelationships) -> None:
        """Tests for DataModelRelationships.get_allowed_values with a ValueError"""
        with pytest.raises(
            ValueError, match="Relationship: 'not_a_relationship' not in dictionary"
        ):
            dmr.get_allowed_values("not_a_relationship")
