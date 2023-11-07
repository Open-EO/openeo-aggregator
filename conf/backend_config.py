from openeo_aggregator.config import AggregatorBackendConfig

config = AggregatorBackendConfig(
    # TODO: eliminate hardcoded openEO Platform references.
    id="aggregator",
    capabilities_title="openEO Platform",
    capabilities_description="openEO Platform, provided through openEO Aggregator Driver",
    enable_basic_auth=False,
)
