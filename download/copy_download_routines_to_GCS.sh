!/bin/bash

# Copy fikes to GCS, from where they will be accessible by dsub tasks.py or dsub_run.sh on a VM that are downloading
# from TCIA

gsutil cp __init__.py gs://idc-dsub-mount/__init__.py
gsutil cp clone_collection.py gs://idc-dsub-mount/clone_collection.py
gsutil cp helpers/cloner.py gs://idc-dsub-mount/helpers/cloner.py
gsutil cp ../helpers/__init__.py gs://idc-dsub-mount/helpers/__init__.py
gsutil cp ../helpers/tcia_helpers.py gs://idc-dsub-mount/helpers/tcia_helpers.py
