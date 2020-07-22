#!/usr/bin/env
import argparse
import sys
import os
import json
import time
from google.cloud import bigquery
from helpers.bq_helpers import load_BQ_from_json
from BQ.schemas.collections_metadata_schema import collections_metadata_schema
from helpers.tcia_helpers import get_collection_descriptions
from helpers.tcia_scrapers import scrape_tcia_collections_page, build_TCIA_to_Description_ID_Table

def build_metadata(args):
    # Get collection descriptions from TCIA
    descriptions = get_collection_descriptions()
    # Scrape the TCIA Data Collections page for collection metadata
    collections = scrape_tcia_collections_page()
    # Get a table to translate between the 'Collection' value in the scraped data, and collection id's
    # used by ETL
    description_id_map = build_TCIA_to_Description_ID_Table(collections, descriptions)

    # Load a table that maps from the collection id in collections to ids used by the TCIA API and
    # by IDC in collection bucket names
    with open(args.file) as f:
        collection_ids = json.load(f)
    rows = []
    for collection_id, collection_data in collections.items():
        collection_data['Collection'] =collection_id
        collection_data['TCIA_CollectionID'] = collection_ids[collection_id]['TCIA_CollectionID']
        collection_data['IDC_CollectionID'] = collection_ids[collection_id]['IDC_CollectionID']
        collection_data['Webapp_CollectionID'] = collection_ids[collection_id]['IDC_CollectionID'].replace('-','_')
        if collection_id in description_id_map:
            collection_data['Description'] = descriptions[description_id_map[collection_id]]
        else:
            collection_data['Description'] = ""
        rows.append(json.dumps(collection_data))
    metadata = '\n'.join(rows)
    # for collection in collections:
    #     metadata = '\n'.join((metadata, json.dumps(collection)))
    # for k, v in collections.items():
    #     metadata = '\n'.join((metadata, json.dumps(v)))
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client()

    metadata = build_metadata(args)
    job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name, args.bqtable_name, metadata, collections_metadata_schema)
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    print("{}: Completed collections metatdata upload \n".format(time.asctime()))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--file', default='{}/{}'.format(os.environ['PWD'], 'lists/collection_ids.json'),
                        help='Table to translate between collection IDs ')
    parser.add_argument('--bqdataset_name', default='idc_tcia_mvp-wave0', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='idc_tcia_collections_metadata', help='BQ table name')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--project', default='idc-dev-etl')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)