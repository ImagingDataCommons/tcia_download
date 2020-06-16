#!/bin/bash

# This is the "normal" way to run dsub. Use run_tasks.py instead

dsub \
    --provider google-v2 \
    --regions us-central1 \
    --project idc-dev-etl \
    --logging gs://idc-dsub-app-logs \
    --image gcr.io/idc-dev-etl/tcia_cloner \
    --mount CLONE_TCIA=gs://idc-dsub-clone-tcia \
    --task ./tasks.tsv 51-53 \
    --command 'python "${CLONE_TCIA}"/clone_collection.py -c "${TCIA_NAME}" -p 4 > "${OUTPUT_FILE}"' \
    --wait
