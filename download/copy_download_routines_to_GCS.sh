#!/bin/bash

# Copy fikes to GCS, from where they will be accessible by dsub tasks.py or dsub_run.sh on a VM that are downloading
# from TCIA

gsutil cp __init__.py clone_collection.py cloner.py gs://idc-dsub-mount
gsutil cp ../helpers/__init__.py ../helpers/tcia_helpers.py gs://idc-dsub-mount/helpers
