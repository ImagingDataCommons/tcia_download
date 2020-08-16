#!/usr/bin/env
import argparse
import os
import sys

from BQ.gen_BQ_data_collection_metadata_table import gen_collections_table

parser =argparse.ArgumentParser()
parser.add_argument('--file', default='{}/{}'.format(os.environ['PWD'], 'lists/collection_ids_mvp_wave0.json'),
                    help='Table to translate between collection IDs ')
parser.add_argument('--bqdataset_name', default='idc_tcia_mvp_wave0', help='BQ dataset name')
parser.add_argument('--bqtable_name', default='idc_tcia_data_collections_metadata', help='BQ table name')
parser.add_argument('--region', default='us-central1', help='Dataset region')
parser.add_argument('--project', default='idc-dev-etl')

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
gen_collections_table(args)