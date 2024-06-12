
# Usage

## Run with Flask in development mode

```shell
export FLASK_APP='openeo_aggregator.app:create_app()'
export FLASK_ENV=development
flask run
```

## Run with Gunicorn

```shell
gunicorn 'openeo_aggregator.app:create_app()'
```
