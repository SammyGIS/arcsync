from .utils.yaml import read_yaml
from config.settings import database


#  Default Extent for Layers 
EXTENT = {
    "xmin": -180,
    "ymin": -90,
    "xmax": 180,
    "ymax": 90,
    "spatialReference": {
        "wkid": 4326
    }
}

#  Default Fields for a  Feature Layer 
DEFAULT_LAYER_FIELDS = [
    {
        "name": "fullname",
        "type": "esriFieldTypeString",
        "alias": "Full Name",
        "sqlType": "sqlTypeVarchar",
        "length": 255,
        "nullable": True,
        "editable": True
    }
]

# Default Tags and Metadata 
DEFAULT_TAGS = ["", "Feature Service", "Point Layer"]
DEFAULT_SNIPPET = "Hosted feature service created dynamically via script."
DEFAULT_DESCRIPTION = "This is an empty feature service to which a point layer will be added."

#  Default Service Capabilities 
DEFAULT_CAPABILITIES = "Create,Query,Update,Delete,Sync"

config = read_yaml()

db_host = config["database"]["host"]
db_port = config["database"]["port"]
debug_mode = config["app"]["debug"]