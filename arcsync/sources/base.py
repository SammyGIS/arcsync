from typing import Protocol, Iterable, Dict

class Source(Protocol):
    def read(self) -> Iterable[Dict]:
        """Yield dict records."""
        ...
