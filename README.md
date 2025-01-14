# openEO Aggregator Driver

Driver to run an openEO back-end that combines the collections and compute power of a set of openEO back-ends.


## Installation

Basic install from source, preferably in some kind of virtual environment:

    pip install .

When planning to do development, it is recommended to install it in development mode (option `-e`) with the `dev` "extra":

    pip install -e .[dev]


## Configuration

The openEO-Aggregator specific configuration,
is grouped by an `AggregatorBackendConfig` container object
(subclass of `OpenEoBackendConfig` as defined in the `openeo-python-driver` framework project).
The most important config value is `aggregator_backends`, which
defines the backends to "aggregate".
See [`src/openeo_aggregator/config/config.py`](src/openeo_aggregator/config/config.py)
for more details and other available configuration options.

Use the env var `OPENEO_BACKEND_CONFIG` to point to the desired config path.
For example, using the example [dummy config](src/openeo_aggregator/config/examples/aggregator.dummy.py)
from the repo:

    export OPENEO_BACKEND_CONFIG=src/openeo_aggregator/config/examples/aggregator.dummy.py

When no valid openEO-Aggregator configuration is set that way, you typically get this error:

    ConfigException: Expected AggregatorBackendConfig but got OpenEoBackendConfig


## Running the webapp

Note, as mentioned above: make sure you point to a valid configuration file
before trying to run the web app.

### Flask dev mode

To run locally in development mode, with standard Flask workflow,
for example (also see `./scripts/run-flask-dev.sh`):

    export FLASK_APP=openeo_aggregator.app
    export FLASK_ENV=development
    flask run

The webapp should be available at http://localhost:5000/openeo/1.2

### With gunicorn

To run the app as gunicorn application, with desired options,
for example (also see `./scripts/run-gunicorn.sh`):

    gunicorn --workers=4 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'

The webapp should be available at http://localhost:8080/openeo/1.2


## Docker image

The [docker](docker) folder has a `Dockerfile` to build a Docker image, e.g.:

    docker build -t openeo-aggregator -f docker/Dockerfile .

This image is built automatically and hosted by VITO at `vito-docker.artifactory.vgt.vito.be/openeo-aggregator`

The image runs the app in gunicorn by default (serving on `127.0.0.1:8000`).

Example usage, with some extra gunicorn settings and the built-in dummy config:

    docker run \
      --rm \
      -p 8080:8080 \
      -e GUNICORN_CMD_ARGS='--bind=0.0.0.0:8080 --workers=2' \
      -e OPENEO_BACKEND_CONFIG=/home/openeo/venv/lib/python3.11/site-packages/openeo_aggregator/config/examples/aggregator.dummy.py \
      vito-docker.artifactory.vgt.vito.be/openeo-aggregator:latest

This webapp should be available at http://localhost:8080/openeo/1.2


## Further configuration

The flask/gunicorn related configuration can be set through
standard flask/gunicorn configuration means
like command line options or env variables, as shown above.

### Gunicorn config

For gunicorn there is an example config at `src/openeo_aggregator/config/examples/gunicorn-config.py`,
for example to be used like this:

    gunicorn --config=src/openeo_aggregator/config/examples/gunicorn-config.py 'openeo_aggregator.app:create_app()'

### Logging

By default, logging is done in JSON format.
You can switch to a simple text-based logging with this env var:

    export OPENEO_AGGREGATOR_SIMPLE_LOGGING=1

## Running tests

You can run the unit tests with pytest, the usual way.

    pytest

This will only pick up the unit tests, which you can find in the directory `tests`.

That is to say, when you run the command `pytest` without specifying a specific directory or file
it will _not_ run the integration tests, only the unit tests.
That is a deliberate choice because the integration tests could take long and they run
against a backend server. We want to avoid that you have to wait a long time for each test run during your development cycle.

So if you want integration tests then you have to run those separately, as described below
in [Running integration tests](#running-integration-tests).

### Running subsets of tests

Pytest provides [various options](https://docs.pytest.org/en/latest/usage.html#specifying-tests-selecting-tests)
to run a subset or just a single test.

Run pytest -h for a quick overview or check the pytest documentation for more information.

Some examples (that can be combined):

- Select by substring of the name of a test with the `-k` option:

        # Run all tests with `collections` in their name
        pytest -k collections

- Skip tests that are marked as slow:

        # Run all tests that do not have the maker "slow": @pytest.mark.slow
        pytest -m "not slow"

### Running integration tests

To make it easier to run the integration test suite against the _default_ backend you can run the following shell script (from the root of your local git repository):

    ./scripts/run-integration-tests.sh

To run the integration test suite against any other OpenEO backend:

- first, specify the backend base URL in environment variable `OPENEO_BACKEND_URL` ,
- then run the tests with `pytest integration-tests/`

For example:

    export OPENEO_BACKEND_URL=http://localhost:8080/
    pytest integration-tests/

### Debugging and troubleshooting tips

- The `tmp_path` fixture provides a [fresh temporary folder for a test to work in](https://docs.pytest.org/en/latest/tmpdir.html).
It is cleaned up automatically, except for the last 3 runs, so you can inspect
generated files post-mortem. The temp folders are typically situated under `/tmp/pytest-of-$USERNAME`.

- To disable pytest's default log/output capturing, to better see what is going on in "real time", add these options:

        --capture=no --log-cli-level=INFO
