
import logging
import pytest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_creds():
    mock_creds = {
        'sheet_service': 'mock_sheet_service',
        'drive_service': 'mock_drive_service',
        'creds': 'mock_creds'
    }
    yield mock_creds