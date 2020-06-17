#!/usr/bin/env python
from google.cloud import storage
from subprocess import run, PIPE
import os
import sys
import argparse
from multiprocessing import Process, Queue


def delete_bucket(bucket_name):
    print("Deleting {}".format(bucket_name))
    result = run(['gsutil', '-m', '-q', 'rm', '-r', 'gs://{}'.format(bucket_name)], stdout=PIPE, stderr=PIPE)
    print("   {} deleted, results: {}".format(bucket_name, result), flush=True)
    if result.returncode:
        print('Delete {} failed: {}'.format(result.stderr))
    return bucket_name


#
# Function run by worker processes
#
def worker(input, output):
    # print('worker args, input: {}, output: {}, project: {}, validate: {}'.format(
    #     input, output, project, validate))
    for arguments in iter(input.get, 'STOP'):
        result = delete_bucket(*arguments)
        output.put(result)


def main(args):
    processes = []
    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    client = storage.Client(project=args.project)
    result = client.list_buckets(project=args.project)
    buckets = [bucket for bucket in result if 'idc-tcia' in bucket.name and not 'idc-tcia-1' in bucket.name]
    # for bucket in buckets:
    #     delete_bucket(args, client, bucket)


    for process in range(args.processes):
        processes.append(
            Process(target=worker, args=(task_queue, done_queue)))
        processes[-1].start()

    # Fill the queue:
    for bucket in buckets:
        task_queue.put((bucket.name,))
    # task_queue.put(('idc-tcia-rider-phantom-pet-ct',))

    # for series in sorted_seriess:
    for process in processes:
        result=done_queue.get()
        print("{} deleted".format(result))

    # Tell child processes to stop
    for process in processes:
        task_queue.put('STOP')


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/BillClifford/.config/gcloud/application_default_config.json'
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--collection', '-c', default='lung-phantom')
    parser.add_argument('--project', '-p', default='idc-dev-etl')
    parser.add_argument('--SA', '-a',
            default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    parser.add_argument('--processes', default=4)
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args)