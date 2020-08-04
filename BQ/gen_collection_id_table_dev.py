#!/usr/bin/env
import argparse
import os
import sys
import json

from BQ.gen_collection_id_table import build_collections_id_table

parser =argparse.ArgumentParser()
parser.add_argument('--save_file', default='{}/BQ/lists/collection_ids_dev.json'.format(os.environ['PWD']),
                    help="File in which to save results")
parser.add_argument('--collections', default='all',
                    help="File containing list of IDC collection IDs or 'all' for all collections")

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
collection_ids = build_collections_id_table(args)

with open(args.save_file, 'w') as f:
    #        print('# This table was generated by gen_collection_id_table.py')
    json.dump(collection_ids, f)