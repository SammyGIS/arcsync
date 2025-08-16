"""

pipeline.py

This module provides a robust ETL pipeline for spatial data that appends point features
from a pandas DataFrame to an ArcGIS Feature Layer using ArcGIS Python API.

Each function is modular and includes runtime error handling to ensure clean failure
and integration into larger pipelines or systems.

Typical workflow:
1. Build a spatial DataFrame from latitude and longitude columns.
2. Align its schema to match a target feature layer.
3. Append features in configurable batch sizes.
4. Verify that features have been added.

Requirements:
    - pandas
    - arcgis.features.GeoAccessor
    - ArcGIS Python API (https://developers.arcgis.com/python/)

Example:
    ```python
    import pandas as pd
    from arcgis.gis import GIS
    from spatial_etl import spatial_etl

    gis = GIS("home")
    layer = gis.content.search("My Feature Layer", "Feature Layer")[0].layers[0]

    df = pd.read_csv("locations.csv")  # Must include 'longitude' and 'latitude' columns

    try:
        count = spatial_etl(df, layer)
        print(f"Layer now has {count} features.")
    except RuntimeError as e:
        print("ETL failed:", e)
    ```
"""

import pandas as pd
from arcgis.features import GeoAccessor, FeatureLayer
from typing import Optional, Dict, List, Tuple


def build_spatial_df(df: pd.DataFrame, sr: int = 4326):
    """
    Convert a DataFrame to a spatially-enabled DataFrame using longitude and latitude.

    Args:
        df (pd.DataFrame): Input DataFrame with 'longitude' and 'latitude' columns.
        sr (int): Spatial reference WKID (default is 4326 for WGS84).

    Returns:
        SpatialDataFrame: A GeoAccessor spatial DataFrame.

    Raises:
        RuntimeError: If spatial conversion fails.
    """
    try:
        return GeoAccessor.from_xy(df, x_column="longitude", y_column="latitude", sr=sr)
    except Exception as e:
        raise RuntimeError(f"Failed to build spatial dataframe: {e}") from e


def get_layer_fields(layer: FeatureLayer) -> set:
    """
    Get all field names defined in the target feature layer.

    Args:
        layer (FeatureLayer): The target ArcGIS layer.

    Returns:
        set: A set of field names.

    Raises:
        RuntimeError: If layer properties cannot be read.
    """
    try:
        return {f["name"] for f in layer.properties.fields}
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve layer fields: {e}") from e


def get_protected_fields(layer: FeatureLayer) -> set:
    """
    Get a set of protected fields that must not be overwritten.

    Args:
        layer (FeatureLayer): The target ArcGIS layer.

    Returns:
        set: Protected field names like OBJECTID and GlobalID.

    Raises:
        RuntimeError: If global ID field cannot be read.
    """
    try:
        protected = {"OBJECTID"}
        gid = (layer.properties.get("globalIdField") or "").strip()
        if gid:
            protected.add(gid)
        return protected
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve protected fields: {e}") from e


def align_fields_to_layer(
    sdf: pd.DataFrame,
    layer: FeatureLayer,
    rename_map: Optional[Dict[str, str]] = None
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Rename and filter DataFrame fields to match the layer's schema.

    Args:
        sdf (pd.DataFrame): Spatial DataFrame.
        layer (FeatureLayer): Target layer.
        rename_map (dict, optional): Mapping of source field names to layer field names.

    Returns:
        Tuple[pd.DataFrame, List[str]]: Aligned DataFrame and list of valid field names.

    Raises:
        RuntimeError: If alignment or renaming fails.
    """
    try:
        if rename_map:
            sdf = sdf.rename(columns=rename_map)

        layer_fields = get_layer_fields(layer)
        protected = get_protected_fields(layer)
        keep_cols = [c for c in sdf.columns if c in layer_fields and c not in protected]
        return sdf, keep_cols
    except Exception as e:
        raise RuntimeError(f"Failed to align fields to layer: {e}") from e


def build_featureset(sdf_batch: pd.DataFrame, keep_cols: List[str]):
    """
    Create a FeatureSet from a spatial DataFrame batch.

    Args:
        sdf_batch (pd.DataFrame): A batch of spatial data.
        keep_cols (List[str]): Valid columns to retain.

    Returns:
        FeatureSet: Features ready to be appended to a layer.

    Raises:
        RuntimeError: If FeatureSet construction fails.
    """
    try:
        cols = keep_cols + ["SHAPE"]
        return sdf_batch[cols].spatial.to_featureset()
    except Exception as e:
        raise RuntimeError(f"Failed to build FeatureSet from batch: {e}") from e


def append_batches(
    sdf: pd.DataFrame,
    keep_cols: List[str],
    layer: FeatureLayer,
    batch_size: int = 1000
):
    """
    Append features from a spatial DataFrame to a layer in batches.

    Args:
        sdf (pd.DataFrame): Full spatial DataFrame.
        keep_cols (List[str]): Fields to append.
        layer (FeatureLayer): Target layer.
        batch_size (int): Number of records per batch.

    Raises:
        RuntimeError: If any batch fails to append.
    """
    n = len(sdf)
    total_added = 0
    errors = []

    try:
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            fs = build_featureset(sdf.iloc[start:end].copy(), keep_cols)
            result = layer.edit_features(adds=fs)

            add_results = result.get("addResults", [])
            total_added += sum(1 for r in add_results if r.get("success"))
            batch_errors = [r.get("error") for r in add_results if not r.get("success")]

            if batch_errors:
                errors.extend(batch_errors)

        if errors:
            raise RuntimeError(f"{len(errors)} adds failed. First error: {errors[0]}")
    except Exception as e:
        raise RuntimeError(f"Failed during batch appending: {e}") from e


def verify_layer_count(layer: FeatureLayer) -> int:
    """
    Get the number of features currently in the layer.

    Args:
        layer (FeatureLayer): The target layer.

    Returns:
        int: Total feature count.

    Raises:
        RuntimeError: If count query fails.
    """
    try:
        return layer.query(return_count_only=True)
    except Exception as e:
        raise RuntimeError(f"Failed to verify layer count: {e}") from e


def run_spatial_etl(
    df: pd.DataFrame,
    layer: FeatureLayer,
    rename_map: Optional[Dict[str, str]] = None,
    sr: int = 4326,
    batch_size: int = 1000
) -> int:
    """
    Full ETL pipeline to append spatial data from a DataFrame to an ArcGIS Feature Layer.

    Args:
        df (pd.DataFrame): DataFrame with 'longitude' and 'latitude' columns.
        layer (FeatureLayer): Target ArcGIS layer.
        rename_map (dict, optional): Mapping of DataFrame to layer field names.
        sr (int): Spatial reference WKID (default: 4326 - WGS84).
        batch_size (int): Number of features to append per batch.

    Returns:
        int: Final count of features in the layer after append.

    Raises:
        RuntimeError: If any step in the ETL process fails.
    """
    try:
        sdf = build_spatial_df(df, sr)
        sdf, keep_cols = align_fields_to_layer(sdf, layer, rename_map)
        append_batches(sdf, keep_cols, layer, batch_size)
        return verify_layer_count(layer)
    except Exception as e:
        raise RuntimeError(f"Spatial ETL failed: {e}") from e
