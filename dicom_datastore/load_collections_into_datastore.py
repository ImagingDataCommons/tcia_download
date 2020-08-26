#!/usr/bin/env

# Load data in some set of collections (stored in GCS buckets), and having some bucket name prefix, into a DICOM server store.
# The third party data of some collections is load conditionally. For this purpose, the BQ idc_tcia_third_party_series table
# is used to identify third party data, and, thus must have been previously generated.

import argparse
import sys
import os
import json
from time import sleep
from googleapiclient.errors import HttpError
from google.cloud import storage
from google.cloud import bigquery
from helpers.dicom_helpers import get_dataset, get_dataset_operation, create_dataset, get_dicom_store, create_dicom_store, import_dicom_instance
from helpers.gcs_helpers import get_series
from helpers.dicomweb_helpers import dicomweb_delete_series

_BASE_URL = "https://healthcare.googleapis.com/v1"

def get_storage_client(project):
    storage_client = storage.Client(project=project)
    return storage_client


# Get a list of buckets to be loaded. We have to mark buckets True or False where False means we should not import
# third party data into the DICOMstore
def get_buckets(args, storage_client):
    if args.collections == "all":
        storage_client = get_storage_client(args.project)
        buckets = [bucket for bucket in storage_client.list_buckets(prefix=args.bucket_prefix, project=args.project)]
        # We need to diffentiate between buckets whose prefix is idc_tcia_1 and those whose prefix is idc_tcia (without)
        # the -1. When args.collections=="all", mark all collections as True, meaning upload 3rd party data for all
        buckets = [[bucket.name, True] for bucket in buckets if not '{}1'.format(args.bucket_prefix) in bucket.name]
    else:
        buckets = []
        with open(args.collections) as f:
            for line in f.readlines():
                if line[0] != '#':
                    line = line.strip().split(',')
                    bucket = ['{}{}'.format(args.bucket_prefix, line[0].lower().replace(' ', '-').replace('_', '-')), line[1].strip()=='True']
                    buckets.append(bucket)
    return buckets

def get_third_party_series(args):
    client = bigquery.Client()
    table_id = "{}.{}".format(args.project, args.thirdpartytable)
    query = """
        SELECT *
        FROM {}
        """.format(table_id)
    query_job = client.query(query)
    UIDs = []
    for row in query_job:
        UIDs.append(row["SeriesInstanceUID"])
    return UIDs


def wait_done(response, args, sleep_time):
    operation = response['name'].split('/')[-1]
    while True:
        result = get_dataset_operation(args.project, args.region, args.gch_dataset_name, operation)
        print("{}".format(result))

        if 'done' in result:
            break
        sleep(sleep_time)
    return result


def import_full_collection(args, bucket):
    try:
        print('Importing {}'.format(bucket))
        content_uri = '{}/dicom/*/*/*.dcm'.format(bucket)
        response = import_dicom_instance(args.project, args.region, args.gch_dataset_name, args.gch_dicomstore_name,
                                         content_uri)
    except HttpError as e:
        err = json.loads(e.content)
        print('Error loading {}; code: {}, message: {}'.format(bucket, err['error']['code'], err['error']['message']))
        if 'resolves to zero GCS objects' in err['error']['message']:
            # An empty collection bucket throws an error
            return
    # stats[bucket]=response
    #
    # with open(args.log,'w') as f:
    #     json.dump(stats, f)
    #
    result = wait_done(response, args, args.period)
    return result


def import_original_collection(client, args, bucket, third_party_series):
    all_series = get_series(client, bucket)
    # We start by importing the entire collection
    result = import_full_collection(args, bucket)

    # Now use DICOMweb to delete the 3rd party series
    count = 0
    print(" Deleting third party series")
    for series in all_series:
        # If it's a 3rd party series, delete it
        if  series.split('/')[-2] in third_party_series:
            study_uid = series.split('/')[-3]
            series_uid = series.split('/')[-2]
            # print('Deleting {}/{}'.format(study_uid, series_uid))
            response = dicomweb_delete_series(
                _BASE_URL, args.project, args.region, args.gch_dataset_name, args.gch_dicomstore_name, study_uid, series_uid
            )
            count += 1
    print(" Deleted {} series".format(count))
    return result


def load_collections(args):
    client = get_storage_client(args.project)
    try:
        dataset = get_dataset(args.project, args.region, args.gch_dataset_name)
    except HttpError:
        response = create_dataset(args.project, args.region, args.gch_dataset_name)

    try:
        datastore = get_dicom_store(args.project, args.region, args.gch_dataset_name, args.gch_dicomstore_name)
    except HttpError:
        # Datastore doesn't exist. Create it
        datastore = create_dicom_store(args.project, args.region, args.gch_dataset_name, args.gch_dicomstore_name)
    pass

    if not os.path.exists(args.log):
        os.mknod(args.log)

    try:
        with open(args.log) as f:
            raw = f.readlines()
        dones = [d.split(':')[0][2:-1] for d in raw[0].split('}}}')]
    except:
#        os.mknod(args.log)
        dones = []

    buckets = get_buckets(args, client)
    third_party_series = get_third_party_series(args)
    for bucket in buckets:
        if not bucket[0] in dones:
            if bucket[1]:
                #Import the entire bucket/collection
                result = import_full_collection(args, bucket[0])
            else:
                result = import_original_collection(client, args, bucket[0], third_party_series)
            with open(args.log, 'a') as f:
                json.dump({bucket[0]: result}, f)


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'],'lists/idc_mvp_wave_0.txt'),
                        help='Collections to import into DICOM store')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--gch_dataset_name', default='idc', help='Dataset name')
    parser.add_argument('--gch_dicomstore_name', default='idc', help='Datastore name')
    parser.add_argument('--project', default='canceridc-data')
    parser.add_argument('--thirdpartytable', default='idc.third_party_series')
    parser.add_argument('--log', default='{}/{}'.format(os.environ['PWD'],'logs/load_dicom_store.log'))
    parser.add_argument('--period', default=30, help="seconds to sleep between checking operation status")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    load_collections(args)