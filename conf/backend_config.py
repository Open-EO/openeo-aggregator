from openeo_driver.config import OpenEoBackendConfig
from openeo_driver.server import build_backend_deploy_metadata

import openeo_aggregator

deploy_metadata = build_backend_deploy_metadata(
    packages=["openeo", "openeo_driver", "openeo_aggregator"],
)

# TODO #112 Merge with `AggregatorConfig`
config = OpenEoBackendConfig(
    id="aggregator",
    capabilities_title="openEO Platform",
    capabilities_description="openEO Platform, provided through openEO Aggregator Driver",
    capabilities_backend_version=openeo_aggregator.about.__version__,
    capabilities_deploy_metadata=deploy_metadata,
    enable_basic_auth=False,
)
