from openeo_aggregator.config import AggregatorBackendConfig

config = AggregatorBackendConfig(
    id="aggregator-dummy",
    capabilities_title="openEO Aggregator Test Dummy",
    capabilities_description="openEO Aggregator Test Dummy",
    enable_basic_auth=True,
)
