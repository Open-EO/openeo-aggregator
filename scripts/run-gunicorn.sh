#!/usr/bin/env bash

gunicorn --workers=2 --threads=2 --bind 0.0.0.0:8080 'openeo_aggregator.app:create_app()'

