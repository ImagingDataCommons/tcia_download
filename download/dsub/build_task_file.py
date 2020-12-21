#!/usr/bin/env python
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

# Build the task.tsv file used by run_tasks.py
# This is in the format expected by dsub but we don't pass to dsub directly lest it launch too many VMs

import os
import argparse

from helpers.tcia_helpers import get_collection_sizes

def main(task_file_name):
    collection_sizes = get_collection_sizes()
    with open(task_file_name,'w') as task_file:
        task_file.write('--env TCIA_NAME\t--output SERIES_STATISTICS\t--output OUTPUT_FILE\n')
        for collection in collection_sizes:
            bucket_name = collection[0].lower().replace(' ','-')
            task = '{}\tgs://idc-dsub-logs/output/{}/series_statistics.log\tgs://idc-dsub-logs/output/{}/stdout.log\n'.format(
                collection[0],
                bucket_name,
                bucket_name
            )
            task_file.write(task)

if __name__ == '__main__' :
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default='{}/tasks.new.tsv'.format(os.environ['PWD']))
    argz = parser.parse_args()
    print(argz)
    main(argz.file)
