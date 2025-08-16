"""


"""

import yaml


def load_settings(path:str="config/settings.yaml"):
    """Read settings yaml file """
    with open(path, "r") as file:
        settings = yaml.safe_load(file)
        return settings
    
def load_schema(path:str="config/schema.yaml"):
    """Read Schea yaml file """
    with open(path, "r") as file:
        schema = yaml.safe_load(file)
        return schema