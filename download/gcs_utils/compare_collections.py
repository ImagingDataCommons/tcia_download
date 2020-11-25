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
from google.cloud import storage
from download.gcs_utils.compare_collection import comp_collection

class Logger(object):
    def __init__(self,file):
        self.terminal = sys.stdout
        self.log = open(file, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        pass

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
        dones = [done[2:].strip('\n') for done in dones if done[0:2] == ">>"]
    except:
        os.mknod(args.dones)
        dones = []

    sys.stdout = Logger(args.dones)

    buckets = get_buckets(args, client)
    for bucket in buckets:
        if not bucket in dones:
            comp_collection(args.project, bucket, bucket.replace('idc-tcia', (args.bucket_prefix)))
            # with open(args.dones, 'a') as f:
            #     f.writelines('{}\n'.format(bucket))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dones', default='./logs/compare_collections_dones.txt', help='File of completed collections')
    parser.add_argument('--collections', default='../lists/idc_mvp_wave_1.txt',
                        help='File listing collections to compare to BQ table, or "all"')
    parser.add_argument('--bucket_prefix', default='idc-tcia-1',
                        help='File listing collections to add to BQ table, or "all"')
    parser.add_argument('--project', default='idc-dev-etl', help="Project of the GCS, BQ and GCH tables")
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    if not args.SA == '':
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    print(os.getcwd())
    compare_collections(args)
