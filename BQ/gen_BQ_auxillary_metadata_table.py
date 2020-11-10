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
from google.cloud import storage
from google.cloud import bigquery
from helpers.bq_helpers import BQ_table_exists, create_BQ_table, load_BQ_from_json
from BQ.schemas.auxilliary_metadata import auxilliary_metadata_schema
from helpers.tcia_helpers import get_TCIA_collections

# Create a dictionary to map from idc collection to tcia API collection name
def get_idc_gcs_to_tcia_api_collection_ID_dict():
        tcia_api = get_TCIA_collections()
        collection_ID_dict = {}
        for collection in tcia_api:
            idc_gcs_collection_name = collection.replace(' ','-').replace('_','-'). lower()
            collection_ID_dict[idc_gcs_collection_name] = collection
        return collection_ID_dict

def get_storage_client(project):
    storage_client = storage.Client(project=project)
    return storage_client

# Get info on each blob in a collection
def get_collection_iterator(args, bucket_name,storage_client):
    bucket = storage_client.bucket(bucket_name,user_project=args.gcs_project)
    pages=storage_client.list_blobs(bucket, prefix="dicom/")
    return pages

# Get a list of the buckets corresponding to collections whose metadata is to be added to the BQ table
def get_buckets(args, storage_client):
    if args.collections == "all":
        buckets = [bucket for bucket in storage_client.list_buckets(prefix=args.bucket_prefix, project=args.gcs_project)]
        buckets = [bucket.name for bucket in buckets if not '{}1'.format(args.bucket_prefix) in bucket.name]
    else:
        with open(args.collections) as f:
            buckets = f.readlines()
        buckets = ['idc-tcia-{}'.format(bucket.strip('\n').lower().replace(' ','-').replace('_','-')) for bucket in buckets if not "#" in bucket]
    return buckets

def upload_metadata(args, BQ_client, bucket_name, idc_gcs_collectionID, tcia_api_collectionID, storage_client):
    print("{}: Uploading metadata for {}".format(time.asctime(), bucket_name), flush=True)
    pages = get_collection_iterator(args, bucket_name, storage_client)

    rows = []
    # print("{}: Started metdata build for {}".format(time.asctime(), bucket_name))
    total_rows=0
    processed_pages=0
    rows = []
    for page in pages.pages:
        instances = list(page)
        for instance in instances:
            SOPInstanceUID = instance.id.split('/')[-2].split('.dcm')[0]
            generation = instance.id.split('/')[-1]
            row = {
                "SOPInstanceUID": SOPInstanceUID,
                "TCIA_API_CollectionID": tcia_api_collectionID,
                # "IDC_GCS_CollectionID": idc_gcs_collectionID,
                "IDC_Webapp_CollectionID": idc_gcs_collectionID.replace('-', '_'),
                "GCS_URL": '{}#{}'.format(instance.public_url.replace('https://storage.googleapis.com','gs:/'),generation),
                "GCS_Bucket": bucket_name,
                "CRDC_UUIDs": {
                    "Study": None,
                    "Series": None,
                    "Instance": None,
                },
                "IDC_Version": args.version,
                "GCS_Region": args.region,
                "GCS_Generation": generation,
                "CRC32C_Hash": instance.crc32c,
                "MD5_Hash": instance.md5_hash,
                "Instance_Size": instance.size,
                "Time_Created": str(instance.time_created),
                "Time_Updated": str(instance.updated),
            }
            rows.append(json.dumps(row))
            total_rows+=1
        processed_pages += 1
        if processed_pages % 50 == 0:
            metadata = '\n'.join(rows)
            result = load_BQ_from_json(BQ_client, args.bq_project, args.bq_dataset_name, args.bq_table_name, metadata,
                                       auxilliary_metadata_schema)
            if not result.errors == None:
                print('****{}: Error {} during BQ upload {}'.format(time.asctime(), bucket_name, result.errors), flush=True)
                return -1
            print("    {}: Completed {} rows {}".format(time.asctime(), total_rows, bucket_name), flush=True)
            rows = []
    metadata = '\n'.join(rows)
    result = load_BQ_from_json(BQ_client, args.bq_project, args.bq_dataset_name, args.bq_table_name, metadata,
                               auxilliary_metadata_schema)
    if not result.errors == None:
        print('****{}: Error {} during BQ upload {}'.format(time.asctime(), bucket_name, result.errors), flush=True)
        return -1
    print("    {}: Completed {} rows {}".format(time.asctime(), total_rows, bucket_name), flush=True)

    return 0


def gen_aux_table(args):
    storage_client = get_storage_client(args.gcs_project)
    BQ_client = bigquery.Client(project=args.bq_project)
    idc_gcs_to_tcia_api_collection_ID_dict = get_idc_gcs_to_tcia_api_collection_ID_dict()

    if not BQ_table_exists(BQ_client, args.bq_project, args.bq_dataset_name, args.bq_table_name):
        try:
            table = create_BQ_table(BQ_client, args.bq_project, args.bq_dataset_name, args.bq_table_name, auxilliary_metadata_schema)
        except:
            print("Error creating table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
                  file=sys.stdout, flush=True)
            print("Failed to create BQ table")
            exit()
    if not os.path.exists(os.path.dirname(args.dones)):
        try:
            os.makedirs(os.path.dirname(args.dones))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    try:
        with open(args.dones) as f:
            dones = f.readlines()
        dones = [done.strip('\n') for done in dones]
    except:
        os.mknod(args.dones)
        dones = []

    buckets = get_buckets(args, storage_client)
    for bucket in buckets:
        if not bucket in dones:
            idc_gc_collectionID = bucket.split(args.bucket_prefix)[1]
            tcia_api_collectionID = idc_gcs_to_tcia_api_collection_ID_dict[idc_gc_collectionID]
            result = upload_metadata(args, BQ_client, bucket, idc_gc_collectionID, tcia_api_collectionID, storage_client)
            print("{}: Completed metatdata upload for {}\n".format(time.asctime(), bucket), flush=True)
            with open(args.dones,'a') as f:
                if result == 0:
                    f.writelines('{}\n'.format(bucket))
                else:
                    f.writelines('****Error: {}\n'.format(bucket))


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'], '../lists/idc_mvp_wave_1.txt'),
                        help='File listing collections to add to BQ table, or "all"')
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--gch_dataset_name', default='idc_tcia_mvp_wave0', help='DICOM dataset name')
    parser.add_argument('--gch_datastore_name', default='idc_tcia', help='DICOM datastore name')
    parser.add_argument('--bq_dataset_name', default='idc_tcia_test', help='BQ dataset name')
    parser.add_argument('--bq_table_name', default='auxilliary_metadata', help='BQ table name')
    parser.add_argument('--region', default='us', help='GCS region')
    parser.add_argument('--gcs_project', default='canceridc-data', help="Project of the GCS tables")
    parser.add_argument('--bq_project', default='idc-dev-etl', help="Project of the BQ table to be created")
    parser.add_argument('--version', default='1', help='IDC version')
    parser.add_argument('--dones', default='./logs/gen_BQ_auxilliary_metadata_table_dones_test.txt', help='File of completed collections')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_aux_table(args)