from pathlib import Path
import json
from typing import Dict
from arcsync.config.schema import validate_settings
from arcsync.sources.csv_source import CsvSource
from arcsync.sources.postgres_source import PostgresSource
from arcsync.sources.gsheet_source import GoogleSheetSource
from arcsync.sinks.arcgis_online import ArcGisOnlineSink
from arcsync.sync.cdc import record_hash, diff_by_hash
from arcsync.utils.logger import get_logger

log = get_logger(__name__)

class SyncManager:
    def __init__(self, cfg: dict, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        validate_settings(cfg)

        self.key_col = cfg["source"].get("key_column", "id")
        self.state_dir = Path(".arcsync").resolve()
        self.state_dir.mkdir(exist_ok=True)
        self.state_file = self.state_dir / f"{cfg['project']}_hashes.json"

    def _source(self):
        st = self.cfg["source"]["type"]
        s = self.cfg["source"]
        if st == "csv":
            return CsvSource(s["path"])
        if st == "postgres":
            return PostgresSource(s["dsn"], s["query"])
        if st == "gsheet":
            return GoogleSheetSource(s["spreadsheet_id"], s.get("worksheet", "Sheet1"))
        raise ValueError(f"Unsupported source.type: {st}")

    def _sink(self):
        t = self.cfg["target"]
        return ArcGisOnlineSink(
            feature_layer_url=t["feature_layer_url"],
            auth=t.get("auth", {}),
            dry_run=self.dry_run
        )

    def _load_state(self) -> Dict[str, str]:
        if not self.state_file.exists():
            return {}
        try:
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception:
            log.warning("State file corrupt; starting fresh.")
            return {}

    def _save_state(self, hashes: Dict[str, str]) -> None:
        self.state_file.write_text(json.dumps(hashes, indent=2), encoding="utf-8")

    def run(self) -> None:
        src = self._source()
        sink = self._sink()

        current_map: Dict[str, dict] = {}
        for rec in src.read():
            if self.key_col not in rec:
                raise KeyError(f"Missing key_column '{self.key_col}' in record: {rec}")
            current_map[str(rec[self.key_col])] = rec

        prev_hash = self._load_state()
        to_insert, to_update = diff_by_hash(current_map, prev_hash)

        log.info("Detected %d inserts, %d updates", len(to_insert), len(to_update))
        sink.upsert(to_insert, to_update)

        # Update state
        new_hashes = {k: record_hash(v) for k, v in current_map.items()}
        self._save_state(new_hashes)
        log.info("Sync completed.")
