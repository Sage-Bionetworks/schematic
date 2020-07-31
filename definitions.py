import os


# this is the project's root directory
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# path to config.yml file
CONFIG_PATH = os.path.join(ROOT_DIR, 'config.yml')

# path to credentials.json file
CREDS_PATH = os.path.join(ROOT_DIR, 'credentials.json')

# path to token.pickle file
TOKEN_PICKLE = os.path.join(ROOT_DIR, 'token.pickle')

# path to data directory
DATA_PATH = os.path.join(ROOT_DIR, 'data', '')