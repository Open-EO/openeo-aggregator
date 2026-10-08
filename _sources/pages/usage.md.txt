# Usage

How to run the aggregator (flask based) webapp?


Note: make sure you [point to a valid configuration file](configuration.md)
before trying to run the web app.


## Flask dev mode

To run locally in development mode, with standard Flask workflow,
for example (also see `./scripts/run-flask-dev.sh`):

```shell
export FLASK_APP=openeo_aggregator.app
export FLASK_ENV=development
flask run
```

The webapp should be available at [http://localhost:5000/openeo/1.2](http://localhost:5000/openeo/1.2).


## With gunicorn

To run the app as gunicorn application, with desired options,
for example (also see `./scripts/run-gunicorn.sh`):

```shell
gunicorn --workers=4 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'
```

The webapp should be available at [http://localhost:8080/openeo/1.2](http://localhost:8080/openeo/1.2).


## Docker image

The [docker](https://github.com/Open-EO/openeo-aggregator/blob/master/docker) folder has a `Dockerfile` to build a Docker image, e.g.:

```shell
docker build -t openeo-aggregator -f docker/Dockerfile .
```

This image is built automatically and hosted by VITO at `vito-docker.artifactory.vgt.vito.be/openeo-aggregator`

The image runs the app in gunicorn by default (serving on `127.0.0.1:8000`).

Example usage, with some extra gunicorn settings and the built-in dummy config:

    docker run \
      --rm \
      -p 8080:8080 \
      -e GUNICORN_CMD_ARGS='--bind=0.0.0.0:8080 --workers=2' \
      -e OPENEO_BACKEND_CONFIG=/home/openeo/venv/lib/python3.11/site-packages/openeo_aggregator/config/examples/aggregator.dummy.py \
      vito-docker.artifactory.vgt.vito.be/openeo-aggregator:latest

This webapp should be available at [http://localhost:8080/openeo/1.2](http://localhost:8080/openeo/1.2).
