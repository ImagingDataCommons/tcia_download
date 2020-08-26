#!/usr/bin/env
import argparse
import sys
import os
from BQ.gen_BQ_analysis_collection_metadata_table import gen_collections_table

parser =argparse.ArgumentParser()
parser.add_argument('--third_party_DOIs_file', default='{}/{}'.format(os.environ['PWD'], 'lists/third_party_series_release.json'),
                    help='Table of series/DOI pairs ')
parser.add_argument('--bqdataset_name', default='idc', help='BQ dataset name')
parser.add_argument('--bqtable_name', default='analysis_collections_metadata', help='BQ table name')
parser.add_argument('--region', default='us', help='Dataset region')
parser.add_argument('--project', default='canceridc-data')

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
gen_collections_table(args)