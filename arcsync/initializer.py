from pathlib import Path
from arcsync.utils.logger import get_logger

log = get_logger(__name__)

_GITIGNORE = """\
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*.so
# Virtual env
.venv/
venv/
# Jupyter
.ipynb_checkpoints/
# Local data
data/
# OS
.DS_Store
Thumbs.db
"""

_SETTINGS_YAML = """\
project: "{project}"
source:
  type: "csv"
  path: "./data/sample.csv"
  key_column: "id"  # unique id for CDC

target:
  type: "arcgis_online"
  # NOTE: point to your Feature Layer service URL (not the portal URL)
  feature_layer_url: "https://services.arcgis.com/<orgId>/arcgis/rest/services/<layerName>/FeatureServer/0"
  auth:
    method: "token"   # "token" or "username_password"
    username: ""
    password: ""
    token: ""         # supply if you already have
    referer: "arcsync"

cdc:
  enabled: true
  mode: "hash"     # "hash" or "timestamp"
  field: ""        # set if mode=timestamp (e.g., 'updated_at')
"""

_SCHEMA_YAML = """\
# Very light schema for settings.yaml
required:
  - project
  - source.type
  - target.type

source_types: ["csv", "postgres", "gsheet"]
target_types: ["arcgis_online"]
"""

_SAMPLE_CSV = """\
id,name,value,updated_at
1,Alice,100,2025-01-01T10:00:00
2,Bob,200,2025-02-01T11:00:00
3,Charlie,300,2025-03-01T12:00:00
"""

_SYNC_EXAMPLE = r'''\
from arcsync.sync.sync_manager import SyncManager
from arcsync.config.loader import load_config

if __name__ == "__main__":
    cfg = load_config("settings.yaml")
    mgr = SyncManager(cfg, dry_run=True)
    mgr.run()
'''

def create_project_scaffold(target_dir: Path, overwrite: bool = False) -> None:
    if target_dir.exists() and not overwrite:
        raise FileExistsError(f"{target_dir} already exists (use --force to overwrite)")

    target_dir.mkdir(parents=True, exist_ok=True)

    (target_dir / "data").mkdir(exist_ok=True)
    (target_dir / ".gitignore").write_text(_GITIGNORE, encoding="utf-8")
    (target_dir / "settings.yaml").write_text(_SETTINGS_YAML.format(project=target_dir.name), encoding="utf-8")
    (target_dir / "schema.yaml").write_text(_SCHEMA_YAML, encoding="utf-8")
    (target_dir / "data" / "sample.csv").write_text(_SAMPLE_CSV, encoding="utf-8")
    (target_dir / "sync_example.py").write_text(_SYNC_EXAMPLE, encoding="utf-8")

    log.info("Project scaffold generated at %s", target_dir)
