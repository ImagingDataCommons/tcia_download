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

# This is the main routine for downloading DICOM data from TCIA.
# It calls dsub to launch a VM on which an instance of clone_collection.py is executed.
# Collections are in buckets under the project specified by the --project param. Buckets are name, starting with
# the value of the --trg param. E.G. if --trg is idc-tcia-1, then the bucket for tcga-brca is idc-tcia-1-tcga-brca.
# Incoming data can be compared against data in existing buckets with prefix specified by the --ref param.


from multiprocessing import Process, Queue
import io
import subprocess
from subprocess import PIPE
from os.path import join
import os, sys
import argparse
import shlex
import time

def run_dsub(tcia_name, dsub_string):
#    stream = os.popen(dsub_string)
#    output = stream.read()
    # shlex splits the command line correctly, so that subprocess.run will take it
    print("Starting {}: {} at {}\n".format(tcia_name, dsub_string,time.asctime()), file=sys.stdout, flush=True)
    results = subprocess.run(shlex.split(dsub_string), stdout=PIPE, stderr=PIPE)
    #print("Output from subprocess: {}".format(results))
    if results.returncode < 0:
        print("Dsub failed with error {}".format(results.returncode), file=sys.stderr, flush=True)
        return (tcia_name,results)

    # We now need to get the status of the task we just launched.
    stderr = str(results.stderr)
    dstat_cmd ="{} --status '*' --full".format(stderr[stderr.find('dstat'):stderr.find("--status")])
    print("{} dstat_cmd: {}".format(tcia_name,dstat_cmd), file=sys.stdout, flush=True)
    results = subprocess.run(shlex.split(dstat_cmd), stdout=PIPE, stderr=PIPE)
    #print("Output from subprocess: {}".format(results))
    if results.returncode < 0:
        print("Dstat failed with error {}".format(results.returncode), file=sys.stderr, flush=True)
        return (tcia_name,results)

    #Now we need to get the internal-id of the Pipelines API
    stdout = str(results.stdout)
    internal_id = stdout[stdout.find('internal-id'):]
    internal_id = internal_id[internal_id.find('projects'):]
    internal_id = internal_id[0:internal_id.find('\\n')]
    print("{} internal_id: {}".format(tcia_name, internal_id), file=sys.stdout, flush=True)

    genomics_command = "gcloud alpha genomics operations describe {}".format(internal_id)
    print("{} genomics status command: {}".format(tcia_name, genomics_command), file=sys.stdout, flush=True)
    arg = shlex.split(genomics_command)

    # Get the VM ID for this task
    while True:
        results = subprocess.run(arg, stdout=PIPE, stderr=PIPE)
        stdout =  str(results.stdout)
        if 'Worker "google-pipelines-worker' in stdout:
            print('{} instance: {}\n'.format(tcia_name, stdout.split('Worker "')[1].split('"')[0]))
            break
        time.sleep(1)

    # Wait for genomics to call the task done
    while True:
        results = subprocess.run(arg, stdout=PIPE, stderr=PIPE)
        #print("Output from subprocess: {}".format(results))
        if results.returncode < 0:
            print("Genomics failed with error {}".format(results.returncode), file=sys.stderr, flush=True)
            return (tcia_name,results)
        stdout =  str(results.stdout)
        done = stdout[stdout.find('done'):]
        done = done[:done.find('\\n')]
        if 'true' in done:
            print("*******task: {}, done: {}, time: {}*******".format(tcia_name, done, time.asctime()), file=sys.stderr, flush=True)
            return (tcia_name)
        else:
            print("task: {}, time: {}".format(tcia_name, time.asctime()), file=sys.stderr, flush=True)
            time.sleep(30)


def worker(input, output):
    for args in iter(input.get, 'STOP'):
        result = run_dsub(*args)
        output.put(result)


def main(args):
    """Feed dsub commands to a group of processes
    Parameters
    ----------
    args: argparse arguments
    """
    
    task_queue = Queue()
    done_queue = Queue()
    
    # Start worker processes
    processes = []
    for i in range(args.processes):
        process = Process(target=worker, args=(task_queue, done_queue))
        process.start()
        processes.append(process)

    # Get the list of possible tasks
    with open(args.file) as f:
        tasks = f.read().splitlines()
        
    task = args.initial
    
#     print(tasks)

    while task < args.initial + args.count:
        current_time = time.strftime("%y%m%d-%H%M%S")
        bucket_name = tasks[task].split('\t')[0].lower().replace(' ','-')
#        series_statistics = "{}.{}.log".format(tasks[task].split('\t')[1].split('.')[0], current_time)
#         series_statistics = "gs://idc-logs/{}/app/{}/series_statistics.{}.log".format(args.version,bucket_name, current_time)
        series_statistics = "gs://idc-etl-processing-dsub-logs/{}/app/{}/series_statistics.{}.log".format(args.version,bucket_name, current_time)
        # logging = "gs://idc-logs/{}/dsub/{}".format(args.version,bucket_name)
        logging = "gs://idc-etl-processing-dsub-logs/{}/dsub/{}".format(args.version, bucket_name)
#        output_file = "{}.{}.log".format(tasks[task].split('\t')[2].split('.')[0], current_time)
        dsub_dict = [
            '/Users/BillClifford/git-home/tcia_download/env/bin/dsub',
            '--provider', 'google-v2',
            '--machine-type', 'n2-standard-2',
            '--ssh',
            '--regions', 'us-central1',
            '--project', 'idc-etl-processing',
            '--logging', logging,
            '--image', 'gcr.io/idc-etl-processing/tcia_cloner',
            '--mount', 'CLONE_TCIA={}'.format('gs://idc-dsub-mount'),
            '--env', 'TCIA_NAME="{}"'.format(tasks[task].split('\t')[0]),
            '--env', 'REF_PREFIX="{}"'.format(args.ref),
            '--env', 'TRG_PREFIX="{}"'.format(args.trg),
            '--env', 'PROJECT="{}"'.format(args.project),
            '--output', 'SERIES_STATISTICS={}'.format(series_statistics),
            '--command',"'" + 'python '+'"${CLONE_TCIA}"'+'/clone_collection.py -c '+'"${TCIA_NAME}"'+' -p {}'.format(args.workers)  + "'"]

        #       print(dsub_dict)
        dsub_string = ' '.join(dsub_dict)
        print('dsub_string: {}'.format(dsub_string))

 #       print(dsub_string)
 #       print(shlex.split(dsub_string))

        if args.processes == 0:
            run_dsub(tasks[task].split('\t')[0], dsub_string)
        else:
            # Enqueue the dsub command   
            # print("Enqueuing {}\n".format(dsub_string), file=sys.stdout, flush=True)
            task_queue.put((tasks[task].split('\t')[0], dsub_string))

        time.sleep(2)
        task += 1

    # Now wait for processes to complete
    try:
        while task > args.initial:
            results = done_queue.get()
            print("Completed {} at {}".format(results, time.asctime()), file=sys.stdout, flush=True)
            task -= 1
        # Tell child processes stop
        for process in processes:
            task_queue.put('STOP')
    except:
        # Something went wrong. Presumable an error message was logged
        print("Killing processes", file=sys.stderr, flush=True)
        for p in processes:
            p.terminate()
            p.join()
            
          
if __name__ == '__main__':
    __spec__ = None
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', '-p', type=int, default=1, help="Number of concurrent dsub tasks")
    parser.add_argument('--initial', '-i', type=int, default=1, help="Index in tasks file  (see --file) of first collection to download, 0 based")
    parser.add_argument('--count', '-c', type=int, default=1, help='Number of collections to download from initial')
    parser.add_argument('--workers', '-w', type=int, help='Number of worker processes in a task (on a VM)', default=1)
    parser.add_argument('--ref', '-r', default='', help='Prefix of reference collection buckets against which to validate. Empty string menas no validation')
    parser.add_argument('--trg', '-t', default='idc-tcia-1-', help='Prefix of target collection buckets, buckets being newly populated')
    parser.add_argument('--version', '-v', default='1', help='Version of target data set(?). Really logging subdirectory name')
    parser.add_argument('--file', '-f', default='{}/tasks.tsv'.format(os.environ['PWD']), help="TSV file of collection IDs and logging URLs. Typically generated by build_task_file.py")
    parser.add_argument('--project', default='idc-dev-etl', help='Project under which to store data ')
    args = parser.parse_args()
    print(args)
    main(args)

