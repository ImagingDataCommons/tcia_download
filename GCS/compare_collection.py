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
import time
from google.cloud import storage
from google.cloud.exceptions import NotFound
from helpers.gcs_helpers import get_studies


#Get info on each blob in a collection
def get_collection_iterator(storage_client, bucket_name, prefix):
    pages = storage_client.list_blobs(bucket_name, prefix=prefix)
    # pages = storage_client.list_blobs(bucket_name, prefix="dicom/")
    return pages


def get_bucket_metadata(storage_client, bucket_name, prefix):
    pages = get_collection_iterator(storage_client, bucket_name, prefix)
    blobs = []

    for page in pages.pages:
        blobs.extend(list(page))
    metadata = {blob.name:blob for blob in blobs}
    return metadata


def compare_instances(client, instance_a, instance_b):
    if instance_a.md5_hash != instance_b.md5_hash:
        print("Differing hashes for instance {}".format(instance_a.md5_hash))

def comp_collection(bucket_a, bucket_b):
    print(">>{}, {}".format(bucket_a, bucket_b), flush=True)
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_a)
        studies_a = get_studies(client, bucket_a)
    except NotFound:
        print("Bucket {} not found".format(bucket_a))
        return
    try:
        bucket = client.get_bucket(bucket_b)
        studies_b = get_studies(client, bucket_b)
    except NotFound:
        print("Bucket {} not found".format(bucket_b))
        return
    for study in studies_a:
        if not study in studies_b:
            print("Study {} not in {}".format(study, bucket_a))
        else:
            metadata_a = get_bucket_metadata(client, bucket_a, study)
            metadata_b = get_bucket_metadata(client, bucket_b, study)
            for instance in metadata_a:
                if not instance in metadata_b:
                    print("Instance {} only in {} ".format(instance, bucket_a))
                else:
                    compare_instances(client, metadata_a[instance], metadata_b[instance])
            for instance in metadata_b:
                if not instance in metadata_a:
                    print("Instance {} only in {} ".format(instance, bucket_b))
    for study in studies_b:
        if not study in studies_a:
            print("Stdy {} not in {}".format(study, bucket_b))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket_a', default='idc-tcia-1-tcga-read',
                        help='Bucket to compare')
    parser.add_argument('--bucket_b', default='idc-tcia-1-test',
                        help='Bucket to compare')
    # parser.add_argument('--region', default='us-central1', help='Dataset region')
    # parser.add_argument('--project', default='idc-dev-etl', help="Project of the GCS, BQ and GCH tables")
    # parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    # if not args.SA == '':
    #     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    comp_collection(args.bucket_a, args.bucket_b)

