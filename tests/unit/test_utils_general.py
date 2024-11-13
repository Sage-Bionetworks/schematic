import pytest
from pathlib import Path
from schematic.utils.general import check_synapse_cache_size  # Replace 'your_module' with the actual module name.

import pytest
from unittest.mock import MagicMock
from unittest import mock

@pytest.mark.parametrize(
    "directory, file_sizes, expected_size",
    [
        ('', [1024], 1024),  # Default directory with 1 KB file
        ("/custom/directory", [2048], 2048),  # Custom directory with 2 KB file
        ('', [], 0),  # Empty directory
        ('', [1024, 2048], 3072),  # Directory with multiple files (1 KB + 2 KB)
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
    mock_rglob = mocker.patch("schematic.utils.general.Path.rglob", return_value=mock_files)
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
