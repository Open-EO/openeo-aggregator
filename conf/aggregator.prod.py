from openeo_aggregator.config import AggregatorConfig
from openeo_driver.users.oidc import OidcProvider

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

configured_oidc_providers = [
    OidcProvider(
        id="egi",
        title="EGI Check-in",
        issuer="https://aai.egi.eu/auth/realms/egi/",
        scopes=_DEFAULT_EGI_SCOPES,
        default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
    ),
    OidcProvider(
        id="egi-legacy",
        title="EGI Check-in (legacy)",
        issuer="https://aai.egi.eu/oidc/",  # TODO: remove old EGI provider refs (issuer https://aai.egi.eu/oidc/)
        scopes=_DEFAULT_EGI_SCOPES,
        default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
    ),
]

ZK_HOSTS = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181"

config = AggregatorConfig(
    config_source=__file__,
    aggregator_backends={
        "vito": "https://openeo.vito.be/openeo/1.0/",
        "eodc": "https://openeo.eodc.eu/v1.0/",
    },
    auth_entitlement_check={"oidc_issuer_whitelist": {
        "https://aai.egi.eu/auth/realms/egi/",
        "https://aai.egi.eu/oidc",  # TODO: remove old EGI provider refs (issuer https://aai.egi.eu/oidc/)
    }},
    configured_oidc_providers=configured_oidc_providers,
    partitioned_job_tracking={
        "zk_hosts": ZK_HOSTS,
    },
    zookeeper_prefix="/openeo/aggregator/",
    memoizer={
        # See `memoizer_from_config` for more details
        "type": "zookeeper",
        "config": {
            "zk_hosts": ZK_HOSTS,
            "default_ttl": 60 * 60,
        }
    },

)
