import yaml

def load_yaml(file_path: str) -> dict:
    with open(file_path, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None

    return config_data