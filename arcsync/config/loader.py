from pathlib import Path
import yaml
from arcsync.utils.logger import get_logger

log = get_logger(__name__)

def load_config(path: str | Path) -> dict:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(p)
    with p.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    log.debug("Loaded config from %s", p)
    return cfg
