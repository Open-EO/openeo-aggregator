import logging
import sys

__version__ = "0.11.0a1"


def log_version_info():
    log = logging.getLogger(__name__)
    log.info(f"openeo-aggregator {__version__} (Python {sys.version}")
