"""
openeo-aggregator Flask app
"""

import os

# TODO: better way to plug in backend driver implementation in `openeo_driver.views`?
os.environ["DRIVER_IMPLEMENTATION_PACKAGE"] = "openeo_aggregator.backend"
import openeo_driver.views

# "flask run" looks for an Flask "app" object
app = openeo_driver.views.app
