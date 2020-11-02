!/bin/bash
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

# Copy fikes to GCS, from where they will be accessible by dsub tasks.py or dsub_run.sh on a VM that are downloading
# from TCIA

gsutil cp __init__.py gs://idc-dsub-mount/__init__.py
gsutil cp clone_collection.py gs://idc-dsub-mount/clone_collection.py
gsutil cp helpers/cloner.py gs://idc-dsub-mount/helpers/cloner.py
gsutil cp ../helpers/__init__.py gs://idc-dsub-mount/helpers/__init__.py
gsutil cp ../helpers/tcia_helpers.py gs://idc-dsub-mount/helpers/tcia_helpers.py
