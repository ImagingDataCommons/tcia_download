#!/usr/bin/env
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

import argparse
import sys
import os
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import BQ_table_exists, create_BQ_table, delete_BQ_Table, load_BQ_from_json
from BQ.identify_third_party_series_file.schemas.third_party_schema import third_party_schema
from BQ.identify_third_party_series_file.identify_third_party_series import id_3rd_party_series

# Create a BQ table of (SeriesInstanceUID, AnalysisDOI) pairs

def gen_collections_table(args):
    BQ_client = bigquery.Client()

    # Always start out with an empty table
    if BQ_table_exists(BQ_client, args.project, args.bq_dataset_name, args.bq_table_name):
        delete_BQ_Table(BQ_client, args.project, args.bq_dataset_name, args.bq_table_name)
    try:
        table = create_BQ_table(BQ_client, args.project, args.bq_dataset_name, args.bq_table_name, third_party_schema)
    except:
        print("Error creating table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
              file=sys.stdout, flush=True)
        print("Failed to create BQ table")
        exit()
    collections, dois, count = id_3rd_party_series(args)
    for collection in collections:
        if len(collections[collection]) > 0:
            ndjson = '\n'.join([json.dumps(series) for series in collections[collection]])
            job = load_BQ_from_json(BQ_client, args.project, args.bq_dataset_name, args.bq_table_name, ndjson, third_party_schema)

            while not job.state == 'DONE':
                print('Status: {}'.format(job.state))
                time.sleep(args.period * 60)
            print("{}: Completed collections metatdata upload for {}".format(time.asctime(), collection))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--dones_file', default='{}/lists/third_party_series_dev.json'.format(os.environ['PWD']),
                        help="File in which to record collected third party DOIs.")
    parser.add_argument('--collections', default='all'.format(os.environ['PWD']),
                        help="File containing list of IDC collection IDs or 'all' for all collections")
    parser.add_argument('--bq_dataset_name', default='idc_tcia_dev', help='BQ dataset name')
    parser.add_argument('--bq_table_name', default='idc_tcia_third_party_seriesx', help='BQ table name')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)