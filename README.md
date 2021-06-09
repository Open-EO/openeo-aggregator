# openEO Aggregator Driver

Driver to run an openEO back-end that combines the collections and compute power of a set of openEO back-ends.

## Install

Basic install from source, preferably in some kind of virtual environment:

    pip install .

When planning to do development, it is recommended to install it in development mode with the `dev` "extra":

    pip install -e .[dev]

## Usage

### Flask dev mode

To run locally in development mode, with standard flask workflow:

    export FLASK_APP=openeo_aggregator.app
    export FLASK_ENV=development
    flask run

The webapp should be available at https://localhost:5000/openeo/1.0

### With gunicorn

To run the app as gunicorn application, with desired options, e.g.:

    gunicorn --workers=2 --threads=2 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'

The webapp should be available at https://localhost:8080/openeo/1.0


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
(for example: command line options or env variables).

Apart from these generic settings, here is also a bit of 
openEO-Aggregator specific configuration, 
which is grouped under the `AggregatorConfig` container.
The most important config value is `aggregator_backends`, which 
defines the backends to "aggregate".
See `config.py`, which also defines a default configuration.

When using the standard flask or gunicorn workflow, a custom configuration
can be set through the `OPENEO_AGGREGATOR_CONFIG` env variable. 
The value of this env variable can be the path to a JSON file holding the configuration values.
For example:

    $ cat config.json
    {"aggregator_backends": {"b1": "https://b1.example", "b2": "https://b2.example"}}
    $ export OPENEO_AGGREGATOR_CONFIG=config.json
    $ gunicorn 'openeo_aggregator.app:create_app()'
    ...
    INFO in openeo_aggregator.app: Using config: {'aggregator_backends': {'b1': 'https://b1.example', 'b2': 'https://b2.example'}}


Or it can be a JSON blob directly:

    $ export OPENEO_AGGREGATOR_CONFIG='{"aggregator_backends":{"b1":"https://b1.example","b2":"https://b2.example"}}'
    $ gunicorn 'openeo_aggregator.app:create_app()'
    ...
    INFO in openeo_aggregator.app: Using config: {'aggregator_backends': {'b1': 'https://b1.example', 'b2': 'https://b2.example'}}


In contexts where the JSON syntax could get lost easily, it is also possible to
pass it URL-encoded (e.g. use `urllib.parse.quote`). 
For example, in the docker run:

    $ docker run --rm -p 8080:8080 \
        -e OPENEO_AGGREGATOR_CONFIG='%7B%22aggregator_backends%22%3A%7B%22b1%22%3A%22https%3A//b1.example%22%2C%22b2%22%3A%22https%3A//b2.example%22%7D%7D' \
        openeo-aggregator
    ...
    INFO in openeo_aggregator.app: Using config: {'aggregator_backends': {'b1': 'https://b1.example', 'b2': 'https://b2.example'}}




    
