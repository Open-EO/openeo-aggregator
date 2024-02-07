from openeo_aggregator.config import AggregatorBackendConfig

config = AggregatorBackendConfig(
    # TODO: eliminate hardcoded openEO Platform references. https://github.com/Open-EO/openeo-aggregator/issues/117
    id="openeo-platform-aggregator",
    capabilities_title="openEO Platform",
    capabilities_description="openEO Platform, provided through openEO Aggregator Driver",
    enable_basic_auth=False,
)
