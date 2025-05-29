import pandas as pd

from schematic.schemas.data_model_parser import DataModelParser
from schematic.utils.schema_utils import parsed_model_as_dataframe
from schematic.schemas.data_model_relationships import DataModelRelationships


class TestSchemaUtils:
    """
    Tests for the schema utils module.
    """

    def test_model_as_dataframe(self) -> None:
        """
        Tests DataModelParser.model_as_dataframe
        """
        # GIVEN a data model
        path_to_data_model = "tests/data/example.model.csv"
        # AND a parser
        parser = DataModelParser(path_to_data_model=path_to_data_model)

        # WHEN the data model is parsed
        result = parser.parse_model()
        # AND the dictionary is unpacked and converted to a DataFrame
        df = parsed_model_as_dataframe(result)

        # THEN the result should be a DataFrame
        assert isinstance(df, pd.DataFrame)
        # AND the keys (attributes) of the dict should be the rows of the DataFrame
        assert list(df.Attribute) == list(result.keys())
