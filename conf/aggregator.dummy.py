"""
Dummy/example config
"""
from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.config import AggregatorConfig

aggregator_config = config = AggregatorConfig(
    config_source=__file__,
    aggregator_backends={
        "dummy": "https://openeo.example/openeo/1.1/",
    },
    configured_oidc_providers=[
        OidcProvider(
            id="egi",
            title="EGI Check-in",
            issuer="https://aai.egi.eu/auth/realms/egi/",
            scopes=["openid"],
        ),
    ],
    auth_entitlement_check=False,
    zookeeper_prefix="/openeo/aggregator/dummy/",
)
