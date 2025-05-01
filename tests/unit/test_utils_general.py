import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Generator
from unittest import mock
from unittest.mock import MagicMock

import pytest
import synapseclient.core.cache as cache
from _pytest.fixtures import FixtureRequest
from synapseclient.core.exceptions import SynapseHTTPError

from schematic.utils import general
from schematic.utils.general import (
    calculate_datetime,
    check_synapse_cache_size,
    clear_synapse_cache,
    entity_type_mapping,
    create_like_statement,
    escape_synapse_path,
)

TEST_DISK_STORAGE = [
    (2, 2),
    (1000, 1000),
    (2000000, 2000000),
    (1073741825, 1073741825),
]


# create temporary files with various size based on request
@pytest.fixture()
def create_temp_query_file(
    tmp_path: Path, request: FixtureRequest
) -> Generator[tuple[Path, Path, Path], None, None]:
    """create temporary files of various size based on request parameter.

    Args:
        tmp_path (Path): temporary file path
        request (any): a request for a fixture from a test

    Yields:
        Generator[Tuple[Path, Path, Path]]: return path of mock synapse cache directory, mock table query folder and csv
    """
    # define location of mock synapse cache
    mock_synapse_cache_dir = tmp_path / ".synapseCache/"
    mock_synapse_cache_dir.mkdir()
    mock_sub_folder = mock_synapse_cache_dir / "123"
    mock_sub_folder.mkdir()
    mock_table_query_folder = mock_sub_folder / "456"
    mock_table_query_folder.mkdir()

    # create mock table query csv
    mock_synapse_table_query_csv = (
        mock_table_query_folder / "mock_synapse_table_query.csv"
    )
    with open(mock_synapse_table_query_csv, "wb") as f:
        f.write(b"\0" * request.param)
    yield mock_synapse_cache_dir, mock_table_query_folder, mock_synapse_table_query_csv


@pytest.mark.parametrize(
    "directory, file_sizes, expected_size",
    [
        ("", [1024], 1024),  # Default directory with 1 KB file
        ("/custom/directory", [2048], 2048),  # Custom directory with 2 KB file
        ("", [], 0),  # Empty directory
        ("", [1024, 2048], 3072),  # Directory with multiple files (1 KB + 2 KB)
    ],
)
def test_check_synapse_cache_size(mocker, directory, file_sizes, expected_size):
    """Test that the file sizes add up to the correct bytes"""
    # Create a list of mocked files based on file_sizes
    mock_files = []
    for size in file_sizes:
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_file.stat.return_value.st_size = size
        mock_files.append(mock_file)

    # Mock Path().rglob() to return the mocked files
    mock_rglob = mocker.patch(
        "schematic.utils.general.Path.rglob", return_value=mock_files
    )
    # Call the function with the directory parameter
    result = check_synapse_cache_size(directory=directory)

    # Assert the result matches the expected size
    assert result == expected_size


@pytest.mark.parametrize(
    "directory",
    [
        None,  # Default directory
        "/custom/directory",  # Custom directory
    ],
)
def test_check_synapse_cache_size_directory_call(directory):
    """Test that the right directory is passed in"""
    with mock.patch("schematic.utils.general.Path") as mock_path:
        mock_rglob = MagicMock(return_value=[])
        mock_path.return_value.rglob = mock_rglob

        # Call the function with the directory parameter
        if directory is None:
            check_synapse_cache_size()
        else:
            check_synapse_cache_size(directory=directory)

        # Assert that Path was called with the correct directory
        expected_directory = directory if directory else "/root/.synapseCache"
        mock_path.assert_called_with(expected_directory)

        # Assert that rglob was called on the Path object
        mock_rglob.assert_called_once_with("*")


class TestGeneral:
    @pytest.mark.parametrize("create_temp_query_file", [3, 1000], indirect=True)
    def test_clear_synapse_cache(self, create_temp_query_file) -> None:
        # define location of mock synapse cache
        (
            mock_synapse_cache_dir,
            mock_table_query_folder,
            mock_synapse_table_query_csv,
        ) = create_temp_query_file
        # create a mock cache map
        mock_cache_map = mock_table_query_folder / ".cacheMap"
        mock_cache_map.write_text(
            f"{mock_synapse_table_query_csv}: '2022-06-13T19:24:27.000Z'"
        )

        assert os.path.exists(mock_synapse_table_query_csv)

        # since synapse python client would compare last modified date and before date
        # we have to create a little time gap here
        time.sleep(1)

        # clear cache
        my_cache = cache.Cache(cache_root_dir=mock_synapse_cache_dir)
        clear_synapse_cache(my_cache, minutes=0.0001)
        # make sure that cache files are now gone
        assert os.path.exists(mock_synapse_table_query_csv) == False
        assert os.path.exists(mock_cache_map) == False

    def test_calculate_datetime_before_minutes(self):
        input_date = datetime.strptime("07/20/23 17:36:34", "%m/%d/%y %H:%M:%S")
        minutes_before = calculate_datetime(
            input_date=input_date, minutes=10, before_or_after="before"
        )
        expected_result_date_before = datetime.strptime(
            "07/20/23 17:26:34", "%m/%d/%y %H:%M:%S"
        )
        assert minutes_before == expected_result_date_before

    def test_calculate_datetime_after_minutes(self):
        input_date = datetime.strptime("07/20/23 17:36:34", "%m/%d/%y %H:%M:%S")
        minutes_after = calculate_datetime(
            input_date=input_date, minutes=10, before_or_after="after"
        )
        expected_result_date_after = datetime.strptime(
            "07/20/23 17:46:34", "%m/%d/%y %H:%M:%S"
        )
        assert minutes_after == expected_result_date_after

    def test_calculate_datetime_raise_error(self):
        with pytest.raises(ValueError):
            input_date = datetime.strptime("07/20/23 17:36:34", "%m/%d/%y %H:%M:%S")
            minutes = calculate_datetime(
                input_date=input_date, minutes=10, before_or_after="error"
            )

    # this test might fail for windows machine
    @pytest.mark.parametrize(
        "create_temp_query_file,local_disk_size",
        TEST_DISK_STORAGE,
        indirect=["create_temp_query_file"],
    )
    def test_check_synapse_cache_size(
        self,
        create_temp_query_file,
        local_disk_size: int,
    ) -> None:
        mock_synapse_cache_dir, _, _ = create_temp_query_file
        disk_size = check_synapse_cache_size(mock_synapse_cache_dir)
        assert disk_size == local_disk_size

    def test_find_duplicates(self):
        mock_list = ["foo", "bar", "foo"]
        mock_dups = {"foo"}

        test_dups = general.find_duplicates(mock_list)
        assert test_dups == mock_dups

    def test_dict2list_with_dict(self):
        mock_dict = {"foo": "bar"}
        mock_list = [{"foo": "bar"}]

        test_list = general.dict2list(mock_dict)
        assert test_list == mock_list

    def test_dict2list_with_list(self):
        # mock_dict = {'foo': 'bar'}
        mock_list = [{"foo": "bar"}]

        test_list = general.dict2list(mock_list)
        assert test_list == mock_list

    @pytest.mark.parametrize(
        "entity_id,expected_type",
        [
            ("syn27600053", "folder"),
            ("syn29862078", "file"),
            ("syn23643253", "asset view"),
            ("syn30988314", "folder"),
            ("syn51182432", "org.sagebionetworks.repo.model.table.TableEntity"),
        ],
    )
    def test_entity_type_mapping(self, synapse_store, entity_id, expected_type):
        syn = synapse_store.syn

        entity_type = entity_type_mapping(syn, entity_id)
        assert entity_type == expected_type

    def test_entity_type_mapping_invalid_entity_id(self, synapse_store):
        syn = synapse_store.syn

        # test with an invalid entity id
        with pytest.raises(SynapseHTTPError) as exception_info:
            entity_type_mapping(syn, "syn123456")

    def test_download_manifest_to_temp_folder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path_dir = general.create_temp_folder(tmpdir)
            assert os.path.exists(path_dir)

    @pytest.mark.parametrize(
        "input_string, expected",
        [
            ("synapse/path", "path like 'synapse/path/%'"),
            ("synapse/path's", "path like 'synapse/path''s/%'"),
            ("synapse/path_", "path like 'synapse/path|_/%' escape '|'"),
            ("synapse/path%", "path like 'synapse/path|%/%' escape '|'"),
        ],
    )
    def test_create_like_statement(self, input_string: str, expected: str) -> None:
        """These tests should create like statements with escaped characters

        Args:
            input_string (str): The input synapse path
            expected (str): The expected like statement
        """
        assert create_like_statement(input_string) == expected

    @pytest.mark.parametrize(
        "input_string",
        ["|", "xxx|", "'|", "|%", "|_"],
    )
    def test_create_like_statement_exceptions(self, input_string: str) -> None:
        """These tests should cause ValueErrors

        Args:
            input_string (str): The input synapse path
        """
        with pytest.raises(ValueError, match="Pattern can not contain '|' character."):
            create_like_statement(input_string)

    @pytest.mark.parametrize(
        "input_string, expected",
        [
            ("", ""),
            ("xxx", "xxx"),
            ("xxx'", "xxx''"),
            ("'xxx'", "''xxx''"),
            ("%", "|%"),
            ("%%", "|%|%"),
            ("_", "|_"),
            ("__", "|_|_"),
            ("_%", "|_|%"),
            ("_xxx_", "|_xxx|_"),
            ("%xxx%", "|%xxx|%"),
            ("_'", "|_''"),
            ("%'", "|%''"),
        ],
    )
    def test_escape_synapse_path(self, input_string: str, expected: str) -> None:
        """This test should have an expected pattern that has escaped characters

        Args:
            input_string (str): The input string pattern
            expected (str): The expected result pattern
        """
        assert escape_synapse_path(input_string) == expected
