import requests
from typing import Iterable, Dict

class ApiSource:
    def __init__(self, url: str, method="GET", headers=None, params=None):
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.params = params or {}

    def read(self) -> Iterable[Dict]:
        resp = requests.request(self.method, self.url, headers=self.headers, params=self.params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "results" in data:
            data = data["results"]
        if not isinstance(data, list):
            raise ValueError("API did not return a list of records")
        for rec in data:
            yield rec
