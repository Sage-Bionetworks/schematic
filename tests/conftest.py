import os
import logging
import pytest

from schematic.configuration import CONFIG


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "test_config.yml")
CONFIG.load_config(CONFIG_PATH)


@pytest.fixture()
def mock_creds():
    mock_creds = {
        'sheet_service': 'mock_sheet_service',
        'drive_service': 'mock_drive_service',
        'creds': 'mock_creds'
    }
    yield mock_creds
