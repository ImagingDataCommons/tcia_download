#!/usr/bin/env python
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

from google.cloud import storage
from subprocess import run, PIPE
from google.api_core.exceptions import Conflict
import sys
import argparse

# This script changes the requester pays and/or allAuthenticatedUsers policy of a set of buckets

# Get a list of buckets to be set
def get_buckets(args):
    buckets = []
    with open(args.collections) as f:
        for line in f.readlines():
            if line[0] != '#':
                line = line.strip()
                bucket = '{}{}'.format(args.bucket_prefix, line.lower().replace(' ', '-').replace('_', '-'))
                buckets.append(bucket)
    return buckets


def change_policy(args):
    storage_client = storage.Client()

    buckets = get_buckets(args)

    for bucket_name in buckets:
        # Need to set the user_project in order to change policy when a bucket is already requester_pays
        bucket = storage_client.bucket(bucket_name, user_project=args.project)

        # Enable/disable requester pays
        bucket = storage_client.get_bucket(bucket)
        bucket.requester_pays = args.requester_pays == "True"
        bucket.patch()

        # Add/remove the all
        policy=bucket.get_iam_policy()
        if args.allAuthenticatedUsers == "True":
            if policy.bindings.count({
                "role": "roles/storage.objectViewer",
                "members": {"allAuthenticatedUsers"}
            }) == 0:
                policy.bindings.append({
                    "role": "roles/storage.objectViewer",
                    "members": {"allAuthenticatedUsers"}
                })
        else:
            if policy.bindings.count({
                "role": "roles/storage.objectViewer",
                "members": {"allAuthenticatedUsers"}
            }) == 1:
                policy.bindings.remove({
                    "role": "roles/storage.objectViewer",
                    "members": {"allAuthenticatedUsers"}
                })
        bucket.set_iam_policy(policy)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collections', default='lists/idc_mvp_wave_0.txt',
                        help="File with list of collection ids to be configured")
    parser.add_argument('--bucket_prefix', default='idc-tcia-', help='With collection ids, identify buckets')
    parser.add_argument('--requester_pays', default="True", help="If True, enable requester pays")
    parser.add_argument('--allAuthenticatedUsers', default="True", help="If True, enable allAuthenticatedUsers")
    parser.add_argument('--project', default='canceridc-data', help='Project under which to set policy')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    change_policy(args)