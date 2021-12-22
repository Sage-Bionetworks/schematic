import os
from pathlib import Path
from schematic import CONFIG

#DATA_DIR = os.path.join(os.getcwd(), 'tests', 'data')

class sql_helpers():
    def get_data_path(data_dir, path, *paths):
        return os.path.join(data_dir, path, *paths)

    def make_output_dir():
        parent_path = Path(os.getcwd()).parent
        output_dir = os.path.join(parent_path, 'schematic_sql_outputs')

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        return output_dir

    def make_output_path(output_dir, filename):
        return os.path.join(output_dir, filename + '.csv')

    def parse_config(path, config_filename):
        '''Load a config file and load into a dictionay.
        Args:
            path (str): path to the config file, can be relative or absolute.
        Returns:
            var(dict): dictionary where key-value pairs match those of the config file.
        '''
        path_to_sql_query_config = str(Path(os.path.join(path, config_filename)).resolve())
        var = CONFIG.load_config(path_to_sql_query_config)
        # If dict is nested, flatten it.
        if any(isinstance(i,dict) for i in var.values()):
            var = {k:v for value in var.values() for k, v in value.items()}
        return var