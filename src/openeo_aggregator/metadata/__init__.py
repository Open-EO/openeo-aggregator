# STAC property to use in collection "summaries" and user defined backend selection
# TODO: is this the proper standard field to use? Also see https://github.com/openEOPlatform/architecture-docs/issues/268
STAC_PROPERTY_PROVIDER_BACKEND = "provider:backend"


# https://github.com/Open-EO/openeo-api/tree/draft/extensions/federation#resources-supported-only-by-a-subset-of-back-ends
# Every discoverable resource that is defined as an object and allows to contain additional properties,
# can list the backends that support or host the exposed resource/functionality.
STAC_PROPERTY_FEDERATION_BACKENDS = "federation:backends"
