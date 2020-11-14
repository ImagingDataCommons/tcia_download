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

def get_bucket_info(bucket_name, project, storage_client):
    # print('get_series_info args, bucket_name: {}, study: {}, series: {}, storage_client: {}'.format(
    #     bucket_name, study, series, storage_client
    # ))
    blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs()
    bucket_info = {blob.name: blob.crc32c for blob in blobs}
    return bucket_info

def bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, production, client):
    # Try to create the destination bucket
    new_bucket = client.bucket(dst_bucket_name, user_project=args.dst_project)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = True
    try:
        result = client.create_bucket(new_bucket, requester_pays=production, location='us')
        # If we get here, this is a new bucket
        if production:
            # Add allAuthenticatedUsers
            policy = new_bucket.get_iam_policy(requested_policy_version=3)
            policy.bindings.append({
                "role": "roles/storage.objectViewer",
                "members": {"allAuthenticatedUsers"}
            })
            new_bucket.set_iam_policy(policy)

        # Enable logging
        result = run(['gsutil',  '-u', args.dst_project, 'set', 'on', '-b', "gs://canceridc-data-storage-logs", "gs://idc-tcia-nsclc-radiomics"])
        return(0)
    except Conflict:
        # Bucket exists
        return(1)
    except:
        # Bucket creation failed somehow
        print("Error creating bucket {}: {}".format(dst_bucket_name, result), flush=True)
        return(-1)


def dup_collection(src_bucket_name, dst_bucket_name, src_project, dst_project, production, storage_client):
    # print("Checking if {} was copied".format(src_bucket_name))
    result = bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, production, storage_client)
    if result == 0:
        # Not previously copied
        print("Copying {}".format(src_bucket_name), flush=True)
        try:
            result = run(['gsutil', '-u', args.dst_project, '-m', '-q', 'cp', '-r',
                                        'gs://{}/*'.format(src_bucket_name), 'gs://{}/'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
            print("   {} copied, results: {}".format(src_bucket_name, result), flush=True)
            if result.returncode:
                print('Copy {} failed: {}'.format(result.stderr), flush=True)
                return {"bucket": src_bucket_name, "status": -1}
            return {"bucket": src_bucket_name, "status": 0}
        except:
            print("Error in copying {}: {},{},{}".format(src_bucket_name, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
            return {"bucket": src_bucket_name, "status": -1}

    elif result == 1:
        # Partially copied. Run gsutil cp with the -n no-clobber flag
        print("Continue copying {}".format(src_bucket_name), flush=True)
        try:
            result = run(['gsutil', '-m', '-q', 'cp', '-r', '-n',
                    'gs://{}/*'.format(src_bucket_name), 'gs://{}/'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
            print("   {} copied, results: {}".format(src_bucket_name, result), flush=True)
            if result.returncode:
                print('Copy {} failed: {}'.format(result.stderr), flush=True)
                return {"bucket": src_bucket_name, "status": -1}
            return {"bucket": src_bucket_name, "status": 0}
        except:
            print("Error in copying {}: {},{},{}".format(src_bucket_name, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
            return {"bucket": src_bucket_name, "status": -1}
    else:
        return {"bucket": src_bucket_name, "status": -1}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_bucket_name', default='idc-tcia-1-nsclc-radiomics')
    parser.add_argument('--dst_bucket_name', default='idc-tcia-nsclc-radiomics')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--production', type=bool, default='True', help="If a production bucket, enable requester pays, allAuthUsers")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    client = storage.Client(project=args.dst_project)

    print("******Validate that new code to enable logging is working******")
    exit()

    dup_collection(args.src_bucket_name, args.dst_bucket_name, args.src_project, args.dst_project, args.production, client)
