from openeo_aggregator.config import AggregatorConfig
from openeo_driver.users.oidc import OidcProvider

DEFAULT_OIDC_CLIENT_EGI = {
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
configured_oidc_providers = [
    OidcProvider(
        id="egi",
        issuer="https://aai.egi.eu/oidc/",
        scopes=[
            "openid", "email",
            "eduperson_entitlement",
            "eduperson_scoped_affiliation",
        ],
        title="EGI Check-in",
        default_client=DEFAULT_OIDC_CLIENT_EGI,  # TODO: remove this legacy experimental field
        default_clients=[DEFAULT_OIDC_CLIENT_EGI],
    ),
    # OidcProvider(
    #     id="egi-dev",
    #     issuer="https://aai-dev.egi.eu/oidc/",
    #     scopes=[
    #         "openid", "email",
    #         "eduperson_entitlement",
    #         "eduperson_scoped_affiliation",
    #     ],
    #     title="EGI Check-in (dev)",
    #     default_client=_DEFAULT_OIDC_CLIENT_EGI,  # TODO: remove this legacy experimental field
    #     default_clients=[_DEFAULT_OIDC_CLIENT_EGI],
    # ),
]

config = AggregatorConfig(
    config_source=__file__,
    aggregator_backends={
        "vito": "https://openeo.vito.be/openeo/1.0/",
        "eodc": "https://openeo.eodc.eu/v1.0/",
        "creo": "https://openeo.creo.vito.be/openeo/1.0/",
    },
    auth_entitlement_check={"oidc_issuer_whitelist": {"https://aai.egi.eu/oidc"}},
    configured_oidc_providers=configured_oidc_providers,
)
