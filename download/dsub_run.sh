#!/bin/bash
#
# Copyright 2020, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
