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
    OidcProvider(
        id="egi-dev",
        title="EGI Check-in (dev)",
        issuer="https://aai-dev.egi.eu/auth/realms/egi/",
        scopes=_DEFAULT_EGI_SCOPES,
        default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
    ),
]

config = AggregatorConfig(
    config_source=__file__,
    aggregator_backends={
        "vito": "https://openeo-dev.vito.be/openeo/1.0/",
        "eodc": "https://openeo-dev.eodc.eu/v1.0/",
        # internal version of https://openeo.creo.vito.be/openeo/1.0/
        "creo": "https://openeo.creo.vgt.vito.be/openeo/1.0",
        # Sentinel Hub OpenEO by Sinergise
        "sentinelhub": "https://lj00amcev7.execute-api.eu-central-1.amazonaws.com/testing/",
    },
    auth_entitlement_check={"oidc_issuer_whitelist": {
        "https://aai.egi.eu/auth/realms/egi/",
        "https://aai.egi.eu/oidc",  # TODO: remove old EGI provider refs (issuer https://aai.egi.eu/oidc/)
    }},
    configured_oidc_providers=configured_oidc_providers,
    partitioned_job_tracking={
        "zk_hosts": "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
    },
    zookeeper_prefix="/openeo/aggregator-dev/",
)
