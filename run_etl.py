import arcgis
import time
import os
from arcgis.features import FeatureLayerCollection
from arcgis.features import GeoAccessor
from arcgis.gis import GIS
from shapely import Point
from schema import layer_fields
import pandas as pd

from dotenv import load_dotenv


load_dotenv()

ARCGIS_SERVER = os.getenv("ARCGIS_SERVER")
ARCGIS_USERNAME = os.getenv("ARCGIS_USERNAME")
ARCIGS_PASSWORD = os.getenv("ARCIGS_PASSWORD")


gis = arcgis.GIS(ARCGIS_SERVER,ARCGIS_USERNAME,ARCIGS_PASSWORD)


# --- CREATE EMPTY SERVICE, THEN ADD A POINT LAYER (READY FOR DATA) ---

# Assumes you're already signed in:
# gis = GIS("home")  # or GIS(url, username, password)

# -----------------------------
# 0) Your attribute fields here
# -----------------------------
# We'll keep your existing fields and ensure OBJECTID is present.
def ensure_oid(fields):
    has_oid = any(
        (f.get("name","").upper() == "OBJECTID") and (f.get("type") == "esriFieldTypeOID")
        for f in fields
    )
    if not has_oid:
        fields = [{
            "name":"OBJECTID",
            "type":"esriFieldTypeOID",
            "alias":"OBJECTID",
            "sqlType":"sqlTypeInteger",
            "nullable": False,
            "editable": False
        }] + list(fields)
    return fields

# <<< replace with your real fields >>>
# Example only; keep your existing layer_fields variable if you already have it.
try:
    layer_fields = layer_fields  # use what you had
except NameError:
    layer_fields = [
        {"name":"fullname","type":"esriFieldTypeString","alias":"Full Name",
         "sqlType":"sqlTypeVarchar","length":255,"nullable":True,"editable":True}
    ]
layer_fields = ensure_oid(layer_fields)

# 1) Unique name (avoid collisions)
unique_name = f"SurveyData_{int(time.time())}"
print("Service name:", unique_name)

# 2) (Optional) remove that known ghost item if it exists
try:
    ghost = gis.content.get("vXaMZMir502uvJfN")
    if ghost:
        print("Deleting ghost item:", ghost.title, ghost.id)
        ghost.delete()
except Exception:
    pass

# 3) Create a fresh folder (correct API: create_folder, not folders.create)
folder = gis.content.create_folder(unique_name)
print("Folder created:", folder.get("id"))

# 4) Item properties (do NOT force "type"; API handles it)
item_properties = {
    "title": unique_name,
    "tags": "ESRI, survey",
    "snippet": "Hosted feature service for survey data",
    "description": "Empty service; point layer will be added via add_to_definition."
}

# 5) Create EMPTY service (no layers yet)
extent = {"xmin": -180, "ymin": -90, "xmax": 180, "ymax": 90, "spatialReference": {"wkid": 4326}}
create_params = {
    "name": unique_name,                      # serviceName (critical)
    "serviceDescription": "Survey data store",
    "capabilities": "Create,Query,Update,Delete,Sync",
    "maxRecordCount": 2000,
    "supportedQueryFormats": "JSON",
    "hasStaticData": False,
    "initialExtent": extent,
    "fullExtent": extent,
    "spatialReference": {"wkid": 4326},
    "allowGeometryUpdates": True,
    "tables": [],
    "layers": []                              # EMPTY now
}

svc_item = gis.content.create_service(
    name=unique_name,                          # also set at top level
    service_type="featureService",
    create_params=create_params,
    item_properties=item_properties,
    folder=folder["id"]
)
print("Empty service created:", svc_item.id, svc_item.url)

# 6) Define the POINT layer to add
feature_layer_definition = {
    "id": 0,
    "name": "MeterInstallations",
    "type": "Feature Layer",
    "geometryType": "esriGeometryPoint",
    "objectIdField": "OBJECTID",
    "displayField": "fullname",
    "fields": layer_fields,
    "extent": extent,
    "allowGeometryUpdates": True,
    "hasZ": False,
    "hasM": False,
    "capabilities": "Create,Query,Update,Delete,Sync"
    # Optional but sometimes helpful:
    # "drawingInfo": {"renderer": {"type": "simple", "symbol": {"type":"esriSMS","style":"esriSMSCircle","size":6}}}
}

# 7) Add the layer with a safe retry (handles portal timing hiccups)
flc = FeatureLayerCollection.fromitem(svc_item)
payload = {"layers": [feature_layer_definition]}

for attempt in range(1, 5):
    try:
        time.sleep(1.5 * attempt)  # brief wait helps provisioning
        flc.manager.add_to_definition(payload)
        print(f"add_to_definition: success on attempt {attempt}")
        break
    except Exception as e:
        print(f"add_to_definition attempt {attempt} failed:", e)
        if attempt == 4:
            raise

# 8) Verify the point layer exists (and print its URL)
flc = FeatureLayerCollection.fromitem(svc_item)  # refresh
if getattr(flc, "layers", None):
    for lyr in flc.layers:
        print("Layer:", lyr.properties.name, "| geometry:", lyr.properties.geometryType, "| URL:", lyr.url)
else:
    raise RuntimeError("Layer was not created — check the layer definition or portal logs.")


flc = FeatureLayerCollection.fromitem(svc_item)
layer = None
for lyr in flc.layers:
    if lyr.properties.name == "MeterInstallations":
        layer = lyr
        break
if layer is None:
    # fallback: take first layer
    layer = flc.layers[0]
print("Target layer:", layer.properties.name, "| geometry:", layer.properties.geometryType)