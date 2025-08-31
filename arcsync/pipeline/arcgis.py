import time
import os
import yaml
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from config.constants import (
    EXTENT,
    DEFAULT_LAYER_FIELDS,
    DEFAULT_TAGS,
    DEFAULT_SNIPPET,
    DEFAULT_DESCRIPTION,
    DEFAULT_CAPABILITIES
)

from dotenv import load_dotenv
load_dotenv()

# ─── ArcGIS Credentials ─────────────────────────────────────────────────────
ARCGIS_SERVER = os.getenv("ARCGIS_SERVER")
ARCGIS_USERNAME = os.getenv("ARCGIS_USERNAME")
ARCGIS_PASSWORD = os.getenv("ARCGIS_PASSWORD")


class ArcGISService:
    """
    ArcGISSericeCreator for ArcGIS Online or Enterprise.
    Handles authentication, folder creation, service publishing, and layer setup.
    """

    def __init__(self):
        """Initialize GIS connection and authenticate."""
        self.gis = GIS(ARCGIS_SERVER, ARCGIS_USERNAME, ARCGIS_PASSWORD)
        self.folder = None
        self.service_item = None
        self.layer = None

    def _create_unique_service_name(self, prefix="SurveyData") -> str:
        """Generate a unique service name using timestamp."""
        return f"{prefix}_{int(time.time())}"

    def _ensure_objectid_field(self, fields: list) -> list:
        """Ensure 'OBJECTID' field is included in the schema."""
        has_oid = any(field.get("name", "").lower() == "objectid" for field in fields)
        if not has_oid:
            fields.insert(0, {
                "name": "OBJECTID",
                "type": "esriFieldTypeOID",
                "alias": "OBJECTID",
                "sqlType": "sqlTypeInteger",
                "nullable": False,
                "editable": False
            })
        return fields

    def _prepare_layer_fields(self, fields: list = None) -> list:
        """Prepare and validate fields for the feature layer."""
        return self._ensure_objectid_field(fields or DEFAULT_LAYER_FIELDS)

    def create_folder(self, folder_name: str = None):
        """Create a new folder in ArcGIS content if name is provided."""
        if folder_name:
            self.folder = self.gis.content.create_folder(folder=folder_name)
            print(f"Folder created: {self.folder['title']}")

    def create_feature_service(self, service_name: str = None):
        """
        Create and publish a hosted feature service with a single empty point layer.
        """
        if not service_name:
            service_name = self._create_unique_service_name()

        item_properties = {
            "title": service_name,
            "type": "Feature Service",
            "tags": DEFAULT_TAGS,
            "snippet": DEFAULT_SNIPPET,
            "description": DEFAULT_DESCRIPTION
        }

        feature_service_params = {
            "name": service_name,
            "serviceDescription": DEFAULT_DESCRIPTION,
            "hasStaticData": False,
            "maxRecordCount": 1000,
            "supportedQueryFormats": "JSON",
            "capabilities": DEFAULT_CAPABILITIES,
            "provider": "hosted",
            "geometryType": "esriGeometryPoint",
            "allowGeometryUpdates": True,
            "units": "esriDecimalDegrees",
            "xssPreventionInfo": {"xssPreventionEnabled": True, "xssPreventionRule": "InputOnly", "xssInputRule": "rejectInvalid"},
            "initialExtent": EXTENT,
            "fullExtent": EXTENT,
            "spatialReference": {"wkid": 4326},
            "allowUpdateWithoutMpin": True
        }

        # Create service item
        self.service_item = self.gis.content.create_service(
            name=service_name,
            service_type="featureService",
            create_params=feature_service_params,
            item_properties=item_properties,
            folder=self.folder['title'] if self.folder else None
        )
        print(f"Feature service created: {self.service_item.title}")

    def add_point_layer(self, fields: list = None):
        """Add an empty point layer to the created feature service."""
        if not self.service_item:
            raise ValueError("Feature service has not been created yet.")

        layer_fields = self._prepare_layer_fields(fields)

        try:
            layer_definition = {
                "layers": [{
                    "id": 0,
                    "name": "DIY_Layer",
                    "type": "Feature Layer",
                    "displayField": "fullname",
                    "description": "DIY point layer",
                    "geometryType": "esriGeometryPoint",
                    "extent": EXTENT,
                    "spatialReference": {"wkid": 4326},
                    "fields": layer_fields,
                    "drawingInfo": {
                        "renderer": {
                            "type": "simple",
                            "symbol": {
                                "type": "esriSMS",
                                "style": "esriSMSCircle",
                                "color": [0, 112, 255, 255],
                                "size": 8,
                                "outline": {
                                    "color": [255, 255, 255, 255],
                                    "width": 1
                                }
                            }
                        }
                    },
                    "capabilities": DEFAULT_CAPABILITIES
                }]
            }
        except Exception as e:
            raise RuntimeError(e)

        fl_collection = FeatureLayerCollection.fromitem(self.service_item)
        fl_collection.manager.add_to_definition(layer_definition)
        self.layer = fl_collection.layers[0]
        print(f"Point layer added: {self.layer.properties.name}")

    def run_all(self, folder_name=None, service_name=None, fields=None):
        """
        Run the full workflow to create a hosted feature service and add a layer.
        """
        self.create_folder(folder_name)
        self.create_feature_service(service_name)
        self.add_point_layer(fields)
