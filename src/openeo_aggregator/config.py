from openeo_driver.utils import dict_item


class AggregatorConfig(dict):
    """
    Simple dictionary based configuration for aggregator backend
    """
    aggregator_backends = dict_item()


DEFAULT_CONFIG = AggregatorConfig(
    aggregator_backends={
        "vito": "https://openeo.vito.be/openeo/1.0",
        "eodc": "https://openeo.eodc.eu/v1.0",
    }
)
