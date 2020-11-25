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
import os
import sys
import argparse
from multiprocessing import Process, Queue

from download.gcs_utils.dup_collection import dup_collection

# def get_bucket_info(bucket_name, project, storage_client):
#     # print('get_series_info args, bucket_name: {}, study: {}, series: {}, storage_client: {}'.format(
#     #     bucket_name, study, series, storage_client
#     # ))
#     blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs()
#     bucket_info = {blob.name: blob.crc32c for blob in blobs}
#     return bucket_info
#
# def bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, production, client):
#     # Try to create the destination bucket
#     new_bucket = client.bucket(dst_bucket_name)
#     new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
#     new_bucket.versioning_enabled = True
#     try:
#         result = client.create_bucket(new_bucket, requester_pays=production, location='us')
#         # If we get here, this is a new bucket
#         if production:
#             # Add allAuthenticatedUsers
#             policy = new_bucket.get_iam_policy(requested_policy_version=3)
#             policy.bindings.append({
#                 "role": "roles/storage.objectViewer",
#                 "members": {"allAuthenticatedUsers"}
#             })
#             new_bucket.set_iam_policy(policy)
#         return(0)
#     except Conflict:
#         # Bucket exists
#         return(1)
#     except:
#         # Bucket creation failed somehow
#         print("Error creating bucket {}: {}".format(dst_bucket_name, result), flush=True)
#         return(-1)
#
#
# def copy_bucket(src_bucket_name, dst_bucket_name, src_project, dst_project, production, storage_client):
#     # print("Checking if {} was copied".format(src_bucket_name))
#     result = bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, production, storage_client)
#     if result == 0:
#         # Not previously copied
#         print("Copying {}".format(src_bucket_name), flush=True)
#         try:
#             result = run(['gsutil', '-m', '-q', 'cp', '-r',
#                     'gs://{}/*'.format(src_bucket_name), 'gs://{}/'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
#             print("   {} copied, results: {}".format(src_bucket_name, result), flush=True)
#             if result.returncode:
#                 print('Copy {} failed: {}'.format(result.stderr), flush=True)
#                 return {"bucket": src_bucket_name, "status": -1}
#             return {"bucket": src_bucket_name, "status": 0}
#         except:
#             print("Error in copying {}: {},{},{}".format(src_bucket_name, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
#             return {"bucket": src_bucket_name, "status": -1}
#
#     elif result == 1:
#         # Partially copied. Run gsutil cp with the -n no-clobber flag
#         print("Continue copying {}".format(src_bucket_name), flush=True)
#         try:
#             result = run(['gsutil', '-m', '-q', 'cp', '-r', '-n',
#                     'gs://{}/*'.format(src_bucket_name), 'gs://{}/'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
#             print("   {} copied, results: {}".format(src_bucket_name, result), flush=True)
#             if result.returncode:
#                 print('Copy {} failed: {}'.format(result.stderr), flush=True)
#                 return {"bucket": src_bucket_name, "status": -1}
#             return {"bucket": src_bucket_name, "status": 0}
#         except:
#             print("Error in copying {}: {},{},{}".format(src_bucket_name, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
#             return {"bucket": src_bucket_name, "status": -1}
#     else:
#         return {"bucket": src_bucket_name, "status": -1}


# Function run by worker processes
#
def worker(input, output, project):
    # print('worker args, input: {}, output: {}, project: {}, validate: {}'.format(
    #     input, output, project, validate))
    storage_client = storage.Client(project=project)
    for arguments in iter(input.get, 'STOP'):
        result = dup_collection(*arguments, storage_client)
        output.put(result)

def dup_collections(args):
    processes = []
    # Create queues
    task_queue = Queue()
    done_queue = Queue()
    client = storage.Client(project=args.dst_project)

    # Get a list of the buckets that have already been duplicated
    try:
        with open("{}/{}".format(os.environ['PWD'], args.dones)) as f:
            dones = f.readlines()
        dones = [done.strip('\n') for done in dones]
    except FileNotFoundError:
        dones = []


    # Get a list of all the buckets in the src_project
    result = client.list_buckets(project=args.src_project)
    src_buckets = [bucket.name for bucket in result if args.src_bucket_prefix in bucket.name]

    # Exclude buckets that have already been duplicated
    src_buckets = [bucket for bucket in src_buckets if not bucket in dones]

    if args.processes == 0:
        for bucket in src_buckets:
            src_bucket_name = bucket
            dst_bucket_name = '{}{}'.format(args.dst_bucket_prefix, src_bucket_name.split(args.src_bucket_prefix)[-1])
            '{}{}'.format(args.dst_bucket_prefix, bucket)
            result = copy_bucket(src_bucket_name, dst_bucket_name, args.src_project, args.dst_project, client)
            if result['status'] >=0 :
                with open("{}/{}".format(os.environ['PWD'], args.dones), 'a') as f:
                    f.writelines('{}\n'.format(result['bucket']))
    else:
        # Launch some worker processes
        for process in range(args.processes):
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args.dst_project)))
            processes[-1].start()

        # Fill the queue:
        enqueued_collections = []
        for bucket in src_buckets:
            src_bucket_name = bucket
            dst_bucket_name = '{}{}'.format(args.dst_bucket_prefix, src_bucket_name.split(args.src_bucket_prefix)[-1])
            '{}{}'.format(args.dst_bucket_prefix, bucket)
            task_queue.put((src_bucket_name, dst_bucket_name, args.src_project, args.dst_project, args.production))
            enqueued_collections.append(src_bucket_name)
            # task_queue.put(('idc-tcia-rider-phantom-pet-ct',))

        # Collect results
        while not enqueued_collections == []:
            # Get results of each series. Timeout if waiting too long
            result = done_queue.get()
            enqueued_collections.remove(result['bucket'])
            if result['status'] >=0 :
                with open("{}/{}".format(os.environ['PWD'], args.dones), 'a') as f:
                    f.writelines('{}\n'.format(result['bucket']))

         # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_bucket_prefix', default='idc-tcia-1')
    parser.add_argument('--dst_bucket_prefix', default='idc-tcia')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--production', type=bool, default='True', help="If a production bucket, enable requester pays, allAuthUsers")
    parser.add_argument('--processes', default=4)
    parser.add_argument('--dones', default='GCS/logs/dup_collections_dones.txt', help="List of collections that have been copied")
    parser.add_argument('--SA', '-a',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA

    dup_collections(args)
