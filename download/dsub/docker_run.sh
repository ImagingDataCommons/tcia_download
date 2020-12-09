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

# Run the tcia_cloner docker image locally rather than under dsub
# Parameterization needs to be updater. See run_tasks.py for current parameterization.

COLLECTION=TCGA-READ
BUCKET_NAME=tcga-read

mkdir -p ./idc-dsub-logs/output/${BUCKET_NAME}

docker run \
       -v $HOME/git-home/tcia_download:/root/tcia_download \
       --env TMPDIR=/tmp/data/tmp \
       --env GOOGLE_APPLICATION_CREDENTIALS=/root/tcia_download/application_default_config.json \
       --env SERIES_STATISTICS=/root/tcia_download/idc-dsub-logs/output/${BUCKET_NAME}/series_statistics.log \
       --env OUTPUT_FILE=/root/tcia_download/idc-dsub-logs/output/${BUCKET_NAME}/stdout.log \
       --env CLONE_TCIA=/root/tcia_download/utilities \
       -it tcia_cloner python -m pdb /root/tcia_download/clone_collection.py -c ${COLLECTION}  -p 0
