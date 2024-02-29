"""
Dummy/example config
"""
from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.config import AggregatorBackendConfig, AggregatorConfig

aggregator_config = AggregatorConfig(
    config_source=__file__,
    zookeeper_prefix="/openeo/aggregator/dummy/",
)


config = AggregatorBackendConfig(
    id="aggregator-dummy",
    capabilities_title="openEO Aggregator Dummy",
    capabilities_description="openEO Aggregator Dummy instance.",
    aggregator_backends={
        "dummy": "https://openeo.example/openeo/1.1/",
    },
    oidc_providers=[
        OidcProvider(
            id="egi",
            title="EGI Check-in",
            issuer="https://aai.egi.eu/auth/realms/egi/",
            scopes=["openid"],
        ),
    ],
)
