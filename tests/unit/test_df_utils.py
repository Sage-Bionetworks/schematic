from io import BytesIO

import numpy as np
import pandas as pd
from pandas._libs.parsers import STR_NA_VALUES

from schematic.utils.df_utils import read_csv


class TestReadCsv:
    def test_none_in_na_values(self) -> None:
        # GIVEN a pandas DF that includes a column with a None value
        df = pd.DataFrame({"col1": ["AAA", "BBB", "None"]})

        # AND None is included in the STR_NA_VALUES
        if "None" not in STR_NA_VALUES:
            STR_NA_VALUES.add("None")

        # AND its CSV representation
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # WHEN the CSV is read
        result = read_csv(csv_buffer, na_values=STR_NA_VALUES)

        # THEN the None string value is not preserved
        assert not result.equals(df)
        assert result["col1"][0] == "AAA"
        assert result["col1"][1] == "BBB"
        assert result["col1"][2] is np.nan

    def test_none_not_in_na_values(self) -> None:
        # GIVEN a pandas DF that includes a column with a None value
        df = pd.DataFrame({"col1": ["AAA", "BBB", "None"]})

        # AND its CSV representation
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # WHEN the CSV is read
        result = read_csv(csv_buffer)

        # THEN the None string value is preserved
        assert result.equals(df)
        assert result["col1"][0] == "AAA"
        assert result["col1"][1] == "BBB"
        assert result["col1"][2] == "None"
