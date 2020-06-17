#!/usr/bin/env python
from google.cloud import storage
from subprocess import run, PIPE
from google.api_core.exceptions import Conflict
import os
import sys
import argparse
from multiprocessing import Process, Queue

def get_bucket_info(bucket_name, project, storage_client):
    # print('get_series_info args, bucket_name: {}, study: {}, series: {}, storage_client: {}'.format(
    #     bucket_name, study, series, storage_client
    # ))
    blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs()
    bucket_info = {blob.name: blob.crc32c for blob in blobs}
    return bucket_info

def bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, client):
    # Try to create the destination bucket
    new_bucket = client.bucket(dst_bucket_name)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = True
    try:
        result = client.create_bucket(new_bucket, location='us')
        # result = run(['gsutil', 'mb', '-b', 'on', '-p', dst_project, '-l', 'us-central1', 'gs://{}'.format(dst_bucket_name)],
        #              stdout=PIPE, stderr=PIPE)
        if result.returncode == '200':
            # The bucket did not previously exist
            return(0)
    # except google.cloud.exceptions.Conflict:
    except Conflict:
        # Bucket exists. Check if it is completely copied
        src_blobs = get_bucket_info(src_bucket_name, src_project, client)
        dst_blobs = get_bucket_info(dst_bucket_name, dst_project, client)
        for blob in src_blobs:
            if (not blob in dst_blobs):
                # The bucket is not fully populated. Do it again.
                return(0)
        return(1)
    except:
        # Bucket creation failed somehow
        print("Error creating bucket {}: {}".format(dst_bucket_name, result), flush=True)
        return(-1)


def copy_bucket(src_bucket_name, dst_bucket_name, src_project, dst_project, storage_client):
    result = bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, storage_client)
    if result == 0:
        # Not previously (fully) copied
        print("Copying {}".format(src_bucket_name))
        result = run(['gsutil', '-m', '-q', 'cp', '-r',
                'gs://{}'.format(src_bucket_name), 'gs://{}'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
        print("   {} copied, results: {}".format(dst_bucket_name, result), flush=True)
        if result.returncode:
            print('Copy {} failed: {}'.format(result.stderr), flush=True)
    elif result == 1:
        print("Previously copied {}".format(dst_bucket_name), flush=True)
    return dst_bucket_name


#
# Function run by worker processes
#
def worker(input, output, project):
    # print('worker args, input: {}, output: {}, project: {}, validate: {}'.format(
    #     input, output, project, validate))
    storage_client = storage.Client(project=project)
    for arguments in iter(input.get, 'STOP'):
        result = delete_bucket(*arguments, storage_client)
        output.put(result)

def main(args):
    processes = []
    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    client = storage.Client(project=args.dst_project)
    result = client.list_buckets(project=args.src_project)

    src_buckets = [bucket.name for bucket in result if args.src_bucket_prefix in bucket.name ]

    if args.processes == 0:
        for bucket in src_buckets:
            src_bucket_name = bucket
            dst_bucket_name = '{}{}'.format(args.dst_bucket_prefix, src_bucket_name.split(args.src_bucket_prefix)[-1])
            '{}{}'.format(args.dst_bucket_prefix, bucket)
            dst_bucket_name = 'idc-tcia-rider-phantom-pet-ct'
            result = copy_bucket(src_bucket_name, dst_bucket_name, args.src_project, args.dst_project, client)
    else:
        # Launch some worker processes
        for process in range(args.processes):
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, dst_project)))
            processes[-1].start()

        # Fill the queue:
        for bucket in buckets:
            src_bucket_name = '{}{}'.format(args.src_bucket_prefix, bucket.name)
            dst_bucket_name = '{}{}'.format(args.dst_bucket_prefix, bucket.name)
            task_queue.put((src_bucket_name, dst_bucket_name, args.project))
            # task_queue.put(('idc-tcia-rider-phantom-pet-ct',))

        # Get results
        for process in processes:
            result=done_queue.get()
            print("{} deleted".format(result))

        # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_bucket_prefix', default='idc-tcia-1-')
    parser.add_argument('--dst_bucket_prefix', default='idc-tcia-')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    # parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--processes', default=0)
    parser.add_argument('--SA', '-a',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA

    main(args)
