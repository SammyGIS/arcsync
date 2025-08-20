# Intentionally lightweight; you can upgrade to pydantic later.
def validate_settings(cfg: dict) -> None:
    def _require(path: str):
        cur = cfg
        for key in path.split("."):
            if not isinstance(cur, dict) or key not in cur:
                raise ValueError(f"Missing required setting: {path}")
            cur = cur[key]

    for needed in ["project", "source.type", "target.type"]:
        _require(needed)

    st = cfg["source"]["type"]
    tt = cfg["target"]["type"]
    if st not in {"csv", "postgres", "gsheet"}:
        raise ValueError(f"Unsupported source.type: {st}")
    if tt not in {"arcgis_online"}:
        raise ValueError(f"Unsupported target.type: {tt}")
