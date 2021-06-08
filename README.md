# openEO Aggregator Driver

Driver to run an openEO back-end that combines the collections and compute power of a set of openEO back-ends.

## Usage

To run locally in development mode, with standard flask workflow:

    export FLASK_APP=openeo_aggregator.app
    export FLASK_ENV=development
    flask run

To run the app as gunicorn application:

    gunicorn --workers=2 --threads=2 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'


