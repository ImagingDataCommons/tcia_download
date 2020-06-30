#!/bin/bash

# Copy routines needed by runtasks.py and dsub_run.sh to GCS from where they will be accessible to VMs doing download
# from TCIA

#gsutil cp clone_collection.py helpers/*.py gs://idc-dsub-clone-tcia
gsutil cp clone_collection.py gs://idc-dsub-clone-tcia
gsutil cp helpers/*.py gs://idc-dsub-clone-tcia/helpers
