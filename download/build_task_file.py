#!/usr/bin/env python

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
