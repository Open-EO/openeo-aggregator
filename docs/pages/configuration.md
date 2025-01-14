# Configuration

## Essential configuration

The openEO-Aggregator specific configuration,
is grouped by an `AggregatorBackendConfig` container object
(subclass of `OpenEoBackendConfig` as defined in the `openeo-python-driver` framework project).

The most important config value is `aggregator_backends`, which
defines the backends to "aggregate".
See [`src/openeo_aggregator/config/config.py`](https://github.com/Open-EO/openeo-aggregator/blob/master/src/openeo_aggregator/config/config.py)
for more details and other available configuration options.

Use the env var `OPENEO_BACKEND_CONFIG` to point to the desired config path.
For example, using the example [dummy config](https://github.com/Open-EO/openeo-aggregator/blob/master/src/openeo_aggregator/config/examples/aggregator.dummy.py)
from the repo:

```shell
export OPENEO_BACKEND_CONFIG=src/openeo_aggregator/config/examples/aggregator.dummy.py
```


When no valid openEO-Aggregator is set that way, you typically get this error:

```text
ConfigException: Expected AggregatorBackendConfig but got OpenEoBackendConfig
```


## Further configuration

The flask/gunicorn related configuration can be set through
standard flask/gunicorn configuration means
like command line options or env variables, as shown above.

### Gunicorn config

For running with gunicorn, there is an example config at `src/openeo_aggregator/config/examples/gunicorn-config.py`,
for example to be used like this:

```shell
gunicorn --config=src/openeo_aggregator/config/examples/gunicorn-config.py 'openeo_aggregator.app:create_app()'
```

### Logging

By default, logging is done in JSON format.
You can switch to a simple text-based logging with this env var:

```shell
OPENEO_AGGREGATOR_SIMPLE_LOGGING=1
```
