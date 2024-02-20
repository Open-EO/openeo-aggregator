from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.config import AggregatorBackendConfig

config = AggregatorBackendConfig(
    id="aggregator-dummy",
    capabilities_title="openEO Aggregator Test Dummy",
    capabilities_description="openEO Aggregator Test Dummy",
    enable_basic_auth=True,
    oidc_providers=[
        OidcProvider(id="egi", issuer="https://egi.test", title="EGI"),
        OidcProvider(id="x-agg", issuer="https://x.test", title="X (agg)"),
        OidcProvider(id="y-agg", issuer="https://y.test", title="Y (agg)"),
        OidcProvider(id="z-agg", issuer="https://z.test", title="Z (agg)"),
    ],
    connections_cache_ttl=1.0,
)
