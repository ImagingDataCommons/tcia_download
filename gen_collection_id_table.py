#!/usr/bin/env

import argparse
import sys
import os
import json
import time
from google.cloud import bigquery
from helpers.bq_helpers import BQ_table_exists, create_BQ_table, load_BQ_from_json
from helpers.collections_metadata_schema import collections_metadata_schema
from helpers.tcia_helpers import get_TCIA_collections, get_collection_descriptions, scrape_tcia_collections_page

# Create a table that maps from the collection names in the TCIA Collection Data page,
# https://https://www.cancerimagingarchive.net/collections/ to names used by the TCIA API and
# by IDC in generating bucket names, etc.

def repair_id(src_id):
    if src_id == 'AAPM RT-MAC Grand Challenge 2019':
        dst_id = 'AAPM RT-MAC'
    elif src_id == 'APOLLO-1-VA':
        dst_id = 'APOLLO'
    else:
        dst_id = src_id

    return dst_id


def main(args):
    BQ_client = bigquery.Client()

    # Scrape the TCIA Data Collections page for collection metadata
    collection_ids = {}
    collections = scrape_tcia_collections_page()
    for collection in collections:
        src_id = collection['Collection']
        collection_ids[src_id] = {}
        dst_id = repair_id(src_id)
        collection_ids[src_id]['TCIA_CollectionID'] = \
            dst_id.split('(')[0].split('/')[0].strip(' ').replace(' ','_')
        collection_ids[src_id]['IDC_CollectionID'] = \
            collection_ids[src_id]['TCIA_CollectionID'].lower().replace('_','-')
    for collection in sorted(collection_ids):
        print('{}, {}, {}'.format(
            collection,
            collection_ids[collection]['TCIA_CollectionID'],
            collection_ids[collection]['IDC_CollectionID']))
    with open(args.file,'w') as f:
        json.dump(collection_ids,f)

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--file', default='{}/lists/collection_ids.json'.format(os.environ['PWD']))

    # parser.add_argument('--SA', '-a',
    #         default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    if not args.SA == '':
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    main(args)