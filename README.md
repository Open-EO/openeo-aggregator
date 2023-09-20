# openEO Aggregator Driver

Driver to run an openEO back-end that combines the collections and compute power of a set of openEO back-ends.

## Install

Basic install from source, preferably in some kind of virtual environment:

    pip install .

When planning to do development, it is recommended to install it in development mode with the `dev` "extra":

    pip install -e .[dev]

## Usage

### Flask dev mode

To run locally in development mode, with standard Flask workflow,
for example (also see `./scripts/run-flask-dev.sh`):

    export FLASK_APP=openeo_aggregator.app
    export FLASK_ENV=development
    flask run

The webapp should be available at http://localhost:5000/openeo/1.0

### With gunicorn

To run the app as gunicorn application, with desired options,
for example (also see `./scripts/run-gunicorn.sh`):

    gunicorn --workers=4 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'

The webapp should be available at http://localhost:8080/openeo/1.0


## Docker image

There is a `Dockerfile` to build a Docker image, for example:

    docker build -t openeo-aggregator .

The image runs the app in gunicorn by default, serving on `0.0.0.0:8080`.
For example, to run it locally:

    docker run --rm -p 8080:8080 openeo-aggregator

The webapp should be available at http://localhost:8080/openeo/1.0


## Configuration

The flask/gunicorn related configuration can be set through
standard flask/gunicorn configuration means
like command line options or env variables, as shown above.

### Gunicorn config

For gunicorn there are also configuration files in the `conf` folder.
The production docker based run for examples uses

    gunicorn --config=conf/gunicorn.prod.py openeo_aggregator.app:create_app()

### Application/Flask config

The openEO-Aggregator specific configuration,
is grouped by a `AggregatorConfig` container object.
The most important config value is `aggregator_backends`, which
defines the backends to "aggregate".
See `config.py` for more details and other available configuration options.

The `conf` folder contains config files for the dev and production
variant of this application config:

- `conf/aggregator.dev.py`
- `conf/aggregator.prod.py`

Use the env var `OPENEO_AGGREGATOR_CONFIG` to point to the desired config path.
Currently, the dev config is used as fallback.

Also note that these concrete config files will be refactored out of the `openeo-aggregator` repo
at some point in the future ([#117](https://github.com/Open-EO/openeo-aggregator/issues/117))
and probably only a dummy default config will be preserved.

### Logging

Logging is set up (by default) through `config/logging-json.conf`.

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
