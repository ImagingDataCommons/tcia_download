#!/bin/bash

# Copy fikes to GCS, from where they will be accessible by dsub tasks.py or dsub_run.sh on a VM that are downloading
# from TCIA

gsutil cp clone_collection.py gs://idc-dsub-clone-tcia
gsutil cp helpers/*.py gs://idc-dsub-clone-tcia/helpers
