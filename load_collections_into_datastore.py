#!/usr/bin/env

# Load all collections (buckets) having some bucket name prefix into a DICOM server store

import argparse
import sys
import os
import json
from googleapiclient.errors import HttpError
from google.cloud import storage
from helpers.dicomweb_helpers import get_session
from helpers.gch_helpers import get_dataset
from helpers.dicom_helpers import get_dicom_store, create_dicom_store, import_dicom_instance


def get_storage_client(project):
    storage_client = storage.Client(project=project)
    return storage_client


def get_buckets(args):
    storage_client = get_storage_client(args.project)
    buckets = [bucket for bucket in storage_client.list_buckets(prefix=args.bucket_prefix, project=args.project)]
    buckets = [bucket for bucket in buckets if not '{}1'.format(args.bucket_prefix) in bucket.name]
    return buckets


def dicomweb_search_instance(args, blob, SOPInstanceUID):

    """Handles the GET requests specified in the DICOMweb standard."""
    url = "https://healthcare.googleapis.com/v1/projects/{}/locations/{}".format(args.project, args.region)

    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/instances?SOPInstanceUID={}".format(
        url, args.dataset_name, args.datastore_name, SOPInstanceUID
    )

    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    session = get_session()

    response = session.get(dicomweb_path, headers=headers)
    response.raise_for_status()

    instance = response.json()

    if not len(instance) == 1:
        print('{}/{} not in DICOMweb store'.format(blob.bucket,blob.path))
    # print("Instance:")
    # print(json.dumps(instance, indent=2))


def validate_import(args, bucket):
    blobs = [blob for blob in bucket.list_blobs()]
    for blob in blobs:
        SOPInstanceUID = blob.name.split('/')[-1].split('.dcm')[0]
        dicomweb_search_instance(args, blob, SOPInstanceUID)


def main(args):
    try:
        dataset = get_dataset(args.SA, args.project, args.region, args.dataset_name)
    except HttpError:
        print("Can't access dataset")
        exit(-1)

    try:
        datastore = get_dicom_store(args.project, args.region, args.dataset_name, args.datastore_name)
    except HttpError:
        # Datastore doesn't exist. Create it
        datastore = create_dicom_store(args.project, args.region, args.dataset_name, args.datastore_name)
    pass

    buckets = get_buckets(args)
    for bucket in buckets:
        content_uri = '{}/dicom/*/*/*.dcm'.format(bucket.name)
        try:
            response=import_dicom_instance( args.project, args.region, args.dataset_name, args.datastore_name, content_uri)
            # result = validate_import(args, bucket)
            print('Imported {}'.format(bucket))
        except HttpError as e:
            err=json.loads(e.content)
            print('Error loading {}; code: {}, message: {}'.format(bucket.name, err['error']['code'], err['error']['message']))

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/BillClifford/.config/gcloud/application_default_config.json'
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--dataset_name', '-d', default='idc-tcia', help='Dataset name')
    parser.add_argument('--region', '-r', default='us-central1', help='Dataset region')
    parser.add_argument('--datastore_name', '-s', default='idc-tcia', help='Datastore name')
    parser.add_argument('--project', '-p', default='idc-dev-etl')
    parser.add_argument('--SA', '-a',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args)