#!/usr/bin/env bash

gunicorn --config=src/openeo_aggregator/config/examples/gunicorn-config.py 'openeo_aggregator.app:create_app()'
