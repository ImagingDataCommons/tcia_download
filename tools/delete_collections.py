#!/usr/bin/env python
from google.cloud import storage
from subprocess import run, PIPE
import os
import sys
import argparse

def delete_bucket(args, client, bucket):
    #bucket_name = "{}{}".format(args.bucket_prefix, args.collection)
    #blobs = client.bucket(bucket_name, user_project=args.project).list_blobs()
    # blobs = bucket.list_blobs()
    result = run(['gsutil', '-m', '-q', 'rm', '-r', 'gs://{}'.format(bucket.name)], stdout=PIPE, stderr=PIPE)
    print('{} deleted'.format(bucket.name))

    # try:
    #     result = bucket.delete_blobs(blobs)
    #     result = bucket.delete()
    #     print("Deleted {}".format(bucket.name))
    # except NotFound as e:
    #     print("{} not found".format(e))


def main(args):
    client = storage.Client(project=args.project)
    result = client.list_buckets(project=args.project)
    buckets = [bucket for bucket in result if 'idc-tcia' in bucket.name and not 'idc-tcia-1' in bucket.name]
    for bucket in buckets:
        delete_bucket(args, client, bucket)

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/BillClifford/.config/gcloud/application_default_config.json'
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--collection', '-c', default='lung-phantom')
    parser.add_argument('--project', '-p', default='idc-dev-etl')
    parser.add_argument('--SA', '-a',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args)