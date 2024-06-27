import re

from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.config import AggregatorBackendConfig

_DEFAULT_OIDC_CLIENT_EGI = {
    "id": "openeo-platform-default-client",
    "grant_types": [
        "authorization_code+pkce",
        "urn:ietf:params:oauth:grant-type:device_code+pkce",
        "refresh_token",
    ],
    "redirect_urls": [
        "https://editor.openeo.cloud",
        "http://localhost:1410/",
        "https://editor.openeo.org",
    ],
}

_DEFAULT_EGI_SCOPES = [
    "openid",
    "email",
    "eduperson_entitlement",
    "eduperson_scoped_affiliation",
]

oidc_providers = [
    OidcProvider(
        id="egi",
        title="EGI Check-in",
        issuer="https://aai.egi.eu/auth/realms/egi/",
        scopes=_DEFAULT_EGI_SCOPES,
        default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
    ),
    OidcProvider(
        id="egi-dev",
        title="EGI Check-in (dev)",
        issuer="https://aai-dev.egi.eu/auth/realms/egi/",
        scopes=_DEFAULT_EGI_SCOPES,
        default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
    ),
]

ZK_HOSTS = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181"


config = AggregatorBackendConfig(
    id="openeo-platform-aggregator-dev",
    capabilities_title="openEO Platform (dev)",
    capabilities_description="openEO Platform, provided through openEO Aggregator Driver (development instance).",
    oidc_providers=oidc_providers,
    aggregator_backends={
        "vito": "https://openeo-dev.vito.be/openeo/1.1/",
        "eodc": "https://openeo-dev.eodc.eu/openeo/1.1.0/",
        "creo": "https://openeo-staging.creo.vito.be/openeo/1.1",
        # Sentinel Hub OpenEO by Sinergise
        "sentinelhub": "https://openeo.sentinel-hub.com/production/",
    },
    collection_allow_list=[
        # Special case: only consider Terrascope for SENTINEL2_L2A
        {"collection_id": "SENTINEL2_L2A", "allowed_backends": ["vito"]},
        # Still allow all other collections
        re.compile("(?!SENTINEL2_L2A).*"),
    ],
    zookeeper_prefix="/openeo/aggregator-dev/",
    partitioned_job_tracking={
        "zk_hosts": ZK_HOSTS,
    },
    memoizer={
        # See `memoizer_from_config` for more details
        "type": "chained",
        "config": {
            "parts": [
                {"type": "dict", "config": {"default_ttl": 5 * 60}},
                {
                    "type": "zookeeper",
                    "config": {
                        "zk_hosts": ZK_HOSTS,
                        "default_ttl": 24 * 60 * 60,
                    },
                },
            ]
        },
    },
)
