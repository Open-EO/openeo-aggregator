"""
openeo-aggregator Flask app
"""

import os

# TODO: better way to plug in backend driver implementation in `openeo_driver.views`?
os.environ["DRIVER_IMPLEMENTATION_PACKAGE"] = "openeo_aggregator.backend"
from openeo_driver.views import app

if __name__ == "__main__":
    app.run()
