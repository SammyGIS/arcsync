from typing import Iterable, Dict, List
from arcsync.utils.logger import get_logger

log = get_logger(__name__)

class ArcGisOnlineSink:
    """
    Minimal placeholder sink.
    Implement addFeatures/updateFeatures/deleteFeatures against your Feature Layer URL:
    https://developers.arcgis.com/rest/services-reference/enterprise/feature-service.htm
    """
    def __init__(self, feature_layer_url: str, auth: dict | None = None, dry_run: bool = False):
        self.url = feature_layer_url.rstrip("/")
        self.auth = auth or {}
        self.dry_run = dry_run

    def upsert(self, to_insert: List[Dict], to_update: List[Dict]) -> None:
        if self.dry_run:
            log.info("[DRY-RUN] Would insert %d features; update %d features", len(to_insert), len(to_update))
            return
        # TODO: implement token handling and REST calls to addFeatures/updateFeatures.
        log.warning("ArcGisOnlineSink is a stub. Implement REST calls here.")
