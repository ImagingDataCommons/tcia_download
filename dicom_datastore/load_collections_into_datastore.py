#!/usr/bin/env

# Load all collections (buckets) having some bucket name prefix into a DICOM server store

import argparse
import sys
import os
import json
from time import sleep
from googleapiclient.errors import HttpError
from google.cloud import storage
from helpers.dicom_helpers import get_dataset, get_dataset_operation, create_dataset, get_dicom_store, create_dicom_store, import_dicom_instance


def get_storage_client(project):
    storage_client = storage.Client(project=project)
    return storage_client


# Get a list of buckets to be loaded
def get_buckets(args, storage_client):
    if args.collections == "all":
        storage_client = get_storage_client(args.project)
        buckets = [bucket for bucket in storage_client.list_buckets(prefix=args.bucket_prefix, project=args.project)]
        buckets = [bucket.name for bucket in buckets if not '{}1'.format(args.bucket_prefix) in bucket.name]
    else:
        with open(args.collections) as f:
            buckets = f.readlines()
        buckets = ['{}{}'.format(args.bucket_prefix, bucket.strip('\n').lower().replace(' ','-').replace('_','-')) for bucket in buckets if not "#" in bucket]
    return buckets


def wait_done(bucket, response, args, client):
    operation = response['name'].split('/')[-1]
    while True:
        result = get_dataset_operation(args.project, args.region, args.dataset_name, operation)
        print("{}: {}".format(bucket, result))

        if 'done' in result:
            break
        sleep(args.period)

    print("-------------------------------------")
    with open(args.log, 'a') as f:
        json.dump({bucket: result}, f)
 #       print('\n',file=f)


def load_collections(args):
    client = get_storage_client(args.project)
    try:
        dataset = get_dataset(args.project, args.region, args.dataset_name)
    except HttpError:
        response = create_dataset(args.project, args.region, args.dataset_name)

    try:
        datastore = get_dicom_store(args.project, args.region, args.dataset_name, args.datastore_name)
    except HttpError:
        # Datastore doesn't exist. Create it
        datastore = create_dicom_store(args.project, args.region, args.dataset_name, args.datastore_name)
    pass

    if not os.path.exists(args.log):
        os.mknod(args.log)

    try:
        with open(args.log) as f:
            raw = f.readlines()
        dones = [d.split(':')[0][2:-1] for d in raw[0].split('}}}')]
    except:
        os.mknod(args.dones)
        dones = []

    buckets = get_buckets(args, client)
    for bucket in buckets:
        if not bucket in dones:
            content_uri = '{}/dicom/*/*/*.dcm'.format(bucket)
            try:
                response=import_dicom_instance( args.project, args.region, args.dataset_name, args.datastore_name, content_uri)
                print('Imported {}'.format(bucket))
            except HttpError as e:
                err=json.loads(e.content)
                print('Error loading {}; code: {}, message: {}'.format(bucket, err['error']['code'], err['error']['message']))
                if 'resolves to zero GCS objects' in err['error']['message']:
                    # An empty collection bucket throws an error
                    continue
                break
            # stats[bucket]=response
            #
            # with open(args.log,'w') as f:
            #     json.dump(stats, f)
            #
            wait_done(bucket, response, args, client)


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'],'lists/idc_mvp_wave_0.txt'), help='Collections to import into DICOM store')
    # parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'],'lists/one_collection.txt'), help='Collections to import into DICOM store')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--dataset_name', default='idc_tcia', help='Dataset name')
    parser.add_argument('--datastore_name', default='idc_tcia_mvp_wave0', help='Datastore name')
    parser.add_argument('--project', default='canceridc-data')
    parser.add_argument('--log', default='{}/{}'.format(os.environ['PWD'],'logs/load_dicom_store.log'))
    parser.add_argument('--period', default=30, help="seconds to sleep between checking operation status")
    parser.add_argument('--SA',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    parser.add_argument('--SA', default = '', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    if not args.SA == '':
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    load_collections(args)