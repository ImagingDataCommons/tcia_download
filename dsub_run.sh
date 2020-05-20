#!/bin/bash
dsub \
    --provider google-v2 \
    --project idc-dev-etl \
    --regions us-central1 \
    --logging gs://idc-dsub-logs \
    --image gcr.io/idc-dev-etl/tcia_cloner \
    --mount CLONE_TCIA=gs://idc-dsub-clone-tcia \
    --task ./tasks.tsv 2- \
    --command 'python "${CLONE_TCIA}"/clone_collection.py -c "${NAME}" > "${OUTPUT_FILE}"'
