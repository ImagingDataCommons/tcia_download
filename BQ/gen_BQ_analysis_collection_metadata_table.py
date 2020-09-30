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
from helpers.bq_helpers import load_BQ_from_json
from BQ.schemas.analysis_collections_metadata_schema import analysis_collections_metadata_schema
from helpers.tcia_scrapers import scrape_tcia_analysis_collections_page, build_TCIA_to_Description_ID_Table

# Build a table of all the DOIs in a particular release
def build_DOI_list(args):
    with open(args.third_party_DOIs_file) as f:
        third_party_DOIs = json.load(f)
    DOIs = []
    for collection in third_party_DOIs:
        for map in third_party_DOIs[collection]:
            # DOI = map["SourceDOI"].split('doi.org/')[1]
            DOI = map["SourceDOI"]
            if not DOI in DOIs:
                DOIs.append(DOI)
    return DOIs


def build_metadata(args):
    # Scrape the TCIA Data Collections page for collection metadata
    collection_metadata = scrape_tcia_analysis_collections_page()

    # Get a list of the DOIs of collections that analyzed some set of data collections
    DOIs = build_DOI_list(args)

    rows = []
    for collection_id, collection_data in collection_metadata.items():
        # print(collection_id)
        # if collection_data["DOI"].split('doi.org/')[1] in DOIs:
        if collection_data["DOI"] in DOIs:
            collection_data["Collection"] = collection_id
            # collection_data["DOI"] = collection_data["DOI"].split('doi.org/')[1]
            rows.append(json.dumps(collection_data))
    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client()

    metadata = build_metadata(args)
    job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name, args.bqtable_name, metadata, analysis_collections_metadata_schema)
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    print("{}: Completed collections metatdata upload \n".format(time.asctime()))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--third_party_DOIs_file', default='{}/{}'.format(os.environ['PWD'], 'lists/third_party_series_dev.json'),
                        help='Table of series/DOI pairs ')
    parser.add_argument('--bqdataset_name', default='idc_tcia_dev', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='idc_tcia_analysis_collections_metadata', help='BQ table name')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--project', default='idc-dev-etl')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)