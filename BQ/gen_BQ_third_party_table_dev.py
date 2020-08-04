#!/usr/bin/env
import argparse
import sys
import os
import json
from BQ.gen_BQ_third_party_table import gen_collections_table

# Create a BQ table of (SeriesInstanceUID, AnalysisDOI) pairs

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--dones_file', default='{}/lists/third_party_series_dev.json'.format(os.environ['PWD']),
                        help="Don't generate dones file")
    parser.add_argument('--collections', default="all",
                        help="File containing list of IDC collection IDs or 'all' for all collections")
    parser.add_argument('--bq_dataset_name', default='idc_tcia_dev', help='BQ dataset name')
    parser.add_argument('--bq_table_name', default='idc_tcia_third_party_series', help='BQ table name')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)