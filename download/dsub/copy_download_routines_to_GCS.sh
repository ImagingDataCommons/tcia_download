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

# Copy files to GCS, from where they will be accessible by run_tasks.py instantiation of clone_collection.py on a VM
# that will download a collection from TCIA

gsutil cp __init__.py gs://idc-dsub-mount/__init__.py
gsutil cp clone_collection.py gs://idc-dsub-mount/clone_collection.py
gsutil cp utilities/cloner.py gs://idc-dsub-mount/utilities/cloner.py
gsutil cp ../utilities/__init__.py gs://idc-dsub-mount/utilities/__init__.py
gsutil cp ../utilities/tcia_helpers.py gs://idc-dsub-mount/utilities/tcia_helpers.py
