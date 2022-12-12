#!/usr/bin/env bash

gunicorn --config=conf/gunicorn.dev.py 'openeo_aggregator.app:create_app()'
