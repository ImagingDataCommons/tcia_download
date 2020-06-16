#!/bin/bash

# Copy routines needed by to GCS from where they will be accessible to VMs doing download

#gsutil cp clone_collection.py helpers/*.py gs://idc-dsub-clone-tcia
gsutil cp clone_collection.py gs://idc-dsub-clone-tcia
gsutil cp helpers/*.py gs://idc-dsub-clone-tcia/helpers
