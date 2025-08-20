from pathlib import Path
import pandas as pd
from typing import Iterable, Dict

class CsvSource:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()

    def read(self) -> Iterable[Dict]:
        df = pd.read_csv(self.path)
        for rec in df.to_dict(orient="records"):
            yield rec
