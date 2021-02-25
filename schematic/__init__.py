import logging

from schematic.configuration import CONFIG
from schematic.loader import LOADER

logging.basicConfig(format=('%(levelname)s: [%(asctime)s] %(name)s'
                            ' - %(message)s'),
                    level=logging.WARNING, datefmt='%Y-%m-%d %H:%M:%S')


# Suppress INFO-level logging from some dependencies
logging.getLogger('keyring').setLevel(logging.ERROR)

logger = logging.getLogger('schematic')
