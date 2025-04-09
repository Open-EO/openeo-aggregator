
# Running integration tests

To make it easier to run the integration test suite against the _default_ backend you can run the following shell script (from the root of your local git repository):

    ./scripts/run-integration-tests.sh

To run the integration test suite against any other OpenEO backend:

- first, specify the backend base URL in environment variable `OPENEO_BACKEND_URL` ,
- then run the tests with `pytest integration-tests/`

For example:

    export OPENEO_BACKEND_URL=http://localhost:8080/
    pytest integration-tests/
