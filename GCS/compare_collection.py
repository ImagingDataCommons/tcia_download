#!/usr/bin/env
import argparse
import sys
import os
import time
from google.cloud import storage

# Get info on each blob in a collection
def get_collection_iterator(bucket_name, storage_client):
    pages = storage_client.list_blobs(bucket_name, prefix="dicom/")
    return pages


def get_bucket_metadata(storage_client, bucket_name):
    pages = get_collection_iterator(bucket_name, storage_client)
    blobs = []

    for page in pages.pages:
        blobs.extend(list(page))
    metadata = {blob.name:blob for blob in blobs}
    return metadata

def compare_instances(client, instance_a, instance_b):
    if instance_a.md5_hash != instance_b.md5_hash:
        print("Differing hashes for instance {}".format(instance_a.md5_hash))

def compare_collection(bucket_a, bucket_b):
    print("{}: Comparing {} and {}".format(time.asctime(), bucket_a, bucket_b), flush=True)
    client = storage.Client()
    metadata_a = get_bucket_metadata(client, bucket_a)
    metadata_b = get_bucket_metadata(client, bucket_b)
    for instance in metadata_a:
        if not instance in metadata_b:
            print("Instance {} only in {} ".format(instance, bucket_a))
        else:
            compare_instances(client, metadata_a[instance], metadata_b[instance])
    for instance in metadata_b:
        if not instance in metadata_a:
            print("Instance {} only in {} ".format(instance, bucket_b))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket_a', default='idc-tcia-1-head-neck-cetuximab',
                        help='File listing collections to add to BQ table, or "all"')
    parser.add_argument('--bucket_b', default='idc-tcia-head-neck-cetuximab',
                        help='File listing collections to add to BQ table, or "all"')
    # parser.add_argument('--region', default='us-central1', help='Dataset region')
    # parser.add_argument('--project', default='idc-dev-etl', help="Project of the GCS, BQ and GCH tables")
    # parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    # if not args.SA == '':
    #     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    compare_collection(args.bucket_a, args.bucket_b)

