#!/usr/bin/env bash

gunicorn --workers=4 --threads=1 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'

