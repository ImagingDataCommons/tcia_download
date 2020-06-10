#!/usr/bin/env python

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
    parser.add_argument('--file', '-f', default='tasks.new.tsv')
    argz = parser.parse_args()
    print(argz)
    main(argz.file)
