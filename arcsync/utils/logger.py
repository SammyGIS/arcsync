import logging
import os

def get_logger(name: str) -> logging.Logger:
    level = os.getenv("ARCSYNC_LOGLEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    return logging.getLogger(name)
