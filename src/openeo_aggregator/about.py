import logging
import sys
from typing import Optional

__version__ = "0.17.0a1"


def log_version_info(logger: Optional[logging.Logger] = None):
    logger = logger or logging.getLogger(__name__)
    logger.info(f"openeo-aggregator {__version__} (Python {sys.version})")
