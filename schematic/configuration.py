import os
import yaml


class Configuration(object):
    def __init__(self):
        # path to config.yml file
        self.CONFIG_PATH = None
        # entire configuration data
        self.DATA = None

    def __getattribute__(self, name):
        value = super().__getattribute__(name)
        if value is None and "SCHEMATIC_CONFIG" in os.environ:
            self.load_config_from_env()
            value = super().__getattribute__(name)
        elif value is None and "SCHEMATIC_CONFIG" not in os.environ:
            raise AttributeError(
                "The '%s' configuration field was accessed, but it hasn't been "
                "set yet, presumably because the schematic.CONFIG.load_config() "
                "method hasn't been run yet. Alternatively, you can re-run this "
                "code with the 'SCHEMATIC_CONFIG' environment variable set to "
                "the config.yml file, which will be automatically loaded." % name
            )
        return value

    def __getitem__(self, key):
        return self.DATA[key]

    def get(self, key, default):
        try:
            value = self[key]
        except AttributeError or KeyError:
            value = default
        return value

    @staticmethod
    def load_yaml(file_path: str) -> dict:
        with open(file_path, "r") as stream:
            try:
                config_data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                return None
        return config_data

    def normalize_path(self, path):
        # Retrieve parent directory of the config to decode relative paths
        parent_dir = os.path.dirname(self.CONFIG_PATH)
        # Ensure absolute file paths
        if not os.path.isabs(path):
            path = os.path.join(parent_dir, path)
        # And lastly, normalize file paths
        return os.path.normpath(path)

    def load_config_from_env(self):
        schematic_config = os.environ["SCHEMATIC_CONFIG"]
        print(
            "Loading config YAML file specified in 'SCHEMATIC_CONFIG' "
            "environment variable: %s" % schematic_config
        )
        return self.load_config(schematic_config)

    def load_config(self, config_path=None):
        # If config_path is None, try loading from environment
        if config_path is None and "SCHEMATIC_CONFIG" in os.environ:
            return self.load_config_from_env()
        # Otherwise, raise an error
        elif config_path is None and "SCHEMATIC_CONFIG" not in os.environ:
            raise ValueError(
                "No configuration file provided to the `config_path` argument "
                "in `load_config`()`, nor was one specified in the "
                "'SCHEMATIC_CONFIG' environment variable. Quitting now..."
            )
        # Load configuration YAML file
        config_path = os.path.expanduser(config_path)
        config_path = os.path.abspath(config_path)
        self.DATA = self.load_yaml(config_path)
        self.CONFIG_PATH = config_path
        # Return self.DATA as a side-effect
        return self.DATA

    @property
    def CREDS_PATH(self):
        self._CREDS_PATH = self.DATA["definitions"]["creds_path"]
        self._CREDS_PATH = self.normalize_path(self._CREDS_PATH)
        return self._CREDS_PATH

    @property
    def TOKEN_PICKLE(self):
        self._TOKEN_PICKLE = self.DATA["definitions"]["token_pickle"]
        self._TOKEN_PICKLE = self.normalize_path(self._TOKEN_PICKLE)
        return self._TOKEN_PICKLE

    @property
    def SERVICE_ACCT_CREDS(self):
        self._SERVICE_ACCT_CREDS = self.DATA["definitions"]["service_acct_creds"]
        self._SERVICE_ACCT_CREDS = self.normalize_path(self._SERVICE_ACCT_CREDS)
        return self._SERVICE_ACCT_CREDS

    @property
    def SYNAPSE_CONFIG_PATH(self):
        self._SYNAPSE_CONFIG_PATH = self.DATA["definitions"]["synapse_config"]
        self._SYNAPSE_CONFIG_PATH = self.normalize_path(self._SYNAPSE_CONFIG_PATH)
        return self._SYNAPSE_CONFIG_PATH


CONFIG = Configuration()
