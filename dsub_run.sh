#!/bin/bash
dsub \
    --provider google-v2 \
    --regions us-central1 \
    --project idc-dev-etl \
    --logging gs://idc-dsub-app-logs \
    --image gcr.io/idc-dev-etl/tcia_cloner \
    --mount CLONE_TCIA=gs://idc-dsub-clone-tcia \
    --task ./tasks.tsv 24-27 \
    --command 'python "${CLONE_TCIA}"/clone_collection.py -c "${TCIA_NAME}" -p 4 > "${OUTPUT_FILE}"' \
    --wait
