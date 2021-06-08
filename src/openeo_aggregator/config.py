from openeo_driver.utils import dict_item

STREAM_CHUNK_SIZE_DEFAULT = 10 * 1024


class AggregatorConfig(dict):
    """
    Simple dictionary based configuration for aggregator backend
    """

    # Dictionary mapping backend id to backend url
    aggregator_backends = dict_item()

    auto_logging_setup = dict_item(default=True)
    flask_error_handling = dict_item(default=True)
    streaming_chunk_size = dict_item(default=STREAM_CHUNK_SIZE_DEFAULT)


DEFAULT_CONFIG = AggregatorConfig(
    aggregator_backends={
        "vito": "https://openeo.vito.be/openeo/1.0",
        # "eodc": "https://openeo.eodc.eu/v1.0",
        "eodc-dev": "https://openeo-dev.eodc.eu/v1.0",
    }
)
