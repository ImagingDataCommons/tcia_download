#!/usr/bin/env
import argparse
import sys
import os
import json
from googleapiclient.errors import HttpError

from helpers.gch_helpers import get_dataset
from helpers.dicom_helpers import get_dicom_store, create_dicom_store, import_dicom_instance


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

    content_uri = '{}{}/dicom/*/*/*.dcm'.format(args.bucket_prefix, args.collection)
    try:
        response=import_dicom_instance( args.project, args.region, args.dataset_name, args.datastore_name, content_uri)
        print('Imported {}'.format(content_uri))
    except HttpError as e:
        err=json.loads(e.content)
        print('Error loading {}; code: {}, message: {}'.format(bucket.name, err['error']['code'], err['error']['message']))


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/BillClifford/.config/gcloud/application_default_config.json'
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--collection', '-c', required=True)
    parser.add_argument('--dataset_name', '-d', default='idc-tcia', help='Dataset name')
    parser.add_argument('--region', '-r', default='us-central1', help='Dataset region')
    parser.add_argument('--datastore_name', '-s', default='idc-tcia', help='Datastore name')
    parser.add_argument('--project', '-p', default='idc-dev-etl')
    parser.add_argument('--SA', '-a',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args)