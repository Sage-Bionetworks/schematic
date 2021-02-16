import logging
import pytest

from schematic.manifest.generator import ManifestGenerator
from schematic.schemas.generator import SchemaGenerator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ManifestGenerator:

    def test__init(self, monkeypatch, mock_creds):
        monkeypatch.setattr('schematic.manifest.generator.build_credentials',
                            lambda: mock_creds)

        generator = ManifestGenerator(
            title='mock_title',
            path_to_json_ld='mock_path'
        )

        assert type(generator.title) is str
        assert generator.sheet_service == mock_creds['sheet_service']
        assert generator.root is None
        assert type(generator.sg) is SchemaGenerator

