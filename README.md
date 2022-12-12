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

Which config to pick is determined through env variables:

- if set, env variable `OPENEO_AGGREGATOR_CONFIG` is the path to the desired config file
- otherwise, if set, env variable `ENV` must be `dev` or `prod`
- otherwise, `dev` is used as default

### Logging

Logging is set up (by default) through `config/logging-json.conf`.
