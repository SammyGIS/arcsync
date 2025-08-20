from typing import Iterable, Dict
# Lazy import to keep optional
def _psycopg():
    try:
        import psycopg2
    except Exception as e:
        raise RuntimeError("psycopg2 is required for postgres source") from e
    return psycopg2

class PostgresSource:
    def __init__(self, dsn: str, query: str):
        self.dsn = dsn
        self.query = query

    def read(self) -> Iterable[Dict]:
        psycopg2 = _psycopg()
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(self.query)
                cols = [d[0] for d in cur.description]
                for row in cur:
                    yield dict(zip(cols, row))
