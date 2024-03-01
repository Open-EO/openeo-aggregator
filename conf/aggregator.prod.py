from openeo_driver.users.oidc import OidcProvider

from openeo_aggregator.config import AggregatorBackendConfig, AggregatorConfig

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
    ]
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
]

ZK_HOSTS = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181"

aggregator_config = AggregatorConfig(
    config_source=__file__,
    zookeeper_prefix="/openeo/aggregator/",
    memoizer={
        # See `memoizer_from_config` for more details
        "type": "chained",
        "config": {
            "parts": [
                {
                    "type": "dict",
                    "config": {"default_ttl": 5 * 60}
                },
                {
                    "type": "zookeeper",
                    "config": {
                        "zk_hosts": ZK_HOSTS,
                        "default_ttl": 24 * 60 * 60,
                    }
                }
            ]
        }
    },

)


config = AggregatorBackendConfig(
    id="openeo-platform-aggregator-prod",
    capabilities_title="openEO Platform",
    capabilities_description="openEO Platform, provided through openEO Aggregator Driver.",
    oidc_providers=oidc_providers,
    aggregator_backends={
        "vito": "https://openeo.vito.be/openeo/1.1/",
        "eodc": "https://openeo.eodc.eu/openeo/1.1.0/",
        # Sentinel Hub OpenEO by Sinergise
        "sentinelhub": "https://openeo.sentinel-hub.com/production/",
    },
    partitioned_job_tracking={
        "zk_hosts": ZK_HOSTS,
    },
)
