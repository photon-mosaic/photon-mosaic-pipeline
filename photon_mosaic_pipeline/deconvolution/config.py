from ruamel.yaml import YAML


def read_config(config_yaml_file):
    """Read given yaml file and return dictionary with entries"""
    yaml = YAML(typ="rt")

    # TODO: add handling of file not found error

    with open(config_yaml_file, "r") as file:
        config_dict = yaml.load(file)

    return config_dict
