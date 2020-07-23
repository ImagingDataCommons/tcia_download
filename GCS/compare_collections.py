import argparse
import sys
import os
import json
from google.cloud import storage
from GCS.compare_collection import comp_collection

# Get a list of the buckets corresponding to collections whose metadata is to be added to the BQ table
def get_buckets(args, storage_client):
    if args.collections == "all":
        buckets = [bucket for bucket in storage_client.list_buckets(prefix=args.bucket_prefix, project=args.project)]
        buckets = [bucket.name for bucket in buckets if not '{}1'.format(args.bucket_prefix) in bucket.name]
    else:
        with open(args.collections) as f:
            buckets = f.readlines()
        buckets = ['idc-tcia-{}'.format(bucket.strip('\n').lower().replace(' ','-').replace('_','-')) for bucket in buckets if not "#" in bucket]
    return buckets

def compare_collections(args):
    client = storage.Client()
    try:
        with open(args.dones) as f:
            dones = f.readlines()
        dones = [done.strip('\n') for done in dones]
    except:
        os.mknod(args.dones)
        dones = []

    buckets = get_buckets(args, client)
    for bucket in buckets:
        if not bucket in dones:
            comp_collection(bucket, bucket.replace('idc-tcia-1','idc-tcia'))
            with open(args.dones, 'a') as f:
                f.writelines('{}\n'.format(bucket))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dones', default='./logs/compare_collections_dones.txt', help='File of completed collections')
    parser.add_argument('--collections', default='all',
                        help='File listing collections to add to BQ table, or "all"')
    parser.add_argument('--bucket_prefix', default='idc-tcia-1-',
                        help='File listing collections to add to BQ table, or "all"')
    parser.add_argument('--project', default='idc-dev-etl', help="Project of the GCS, BQ and GCH tables")
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    if not args.SA == '':
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    print(os.getcwd())
    compare_collections(args)
