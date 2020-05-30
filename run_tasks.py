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

    genomics_command = "gcloud alpha genomics operations describe {}\n".format(internal_id)
    print("{} genomics status command: {}".format(tcia_name, genomics_command), file=sys.stdout, flush=True)
    arg = shlex.split(genomics_command)
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


def main(procs, first_task, total_tasks, task_file):
    """Feed dsub commands to a group of processes
    Parameters
    ----------
    procs : int
        Number of processes to run
    first_task : int
        Index in tasks_file of first task to run
    total_tasks : int
        Number of tasks to run 
    task_file : str
        Path to task file
    """
    
    task_queue = Queue()
    done_queue = Queue()
    
    # Start worker processes
    processes = []
    for i in range(procs):
        process = Process(target=worker, args=(task_queue, done_queue))
        process.start()
        processes.append(process)

    # Get the list of possible tasks
    with open(task_file) as f:
        tasks = f.read().splitlines()
        
    task = first_task
    
#     print(tasks)

    while task < first_task + total_tasks:
        current_time = time.strftime("%y%m%d-%H%M%S")
        series_statistics = "{}.{}.log".format(tasks[task].split('\t')[1].split('.')[0], current_time)
        output_file = "{}.{}.log".format(tasks[task].split('\t')[2].split('.')[0], current_time)
        dsub_dict = [
            '/Users/BillClifford/git-home/tcia_download/env/bin/dsub',
            '--provider', 'google-v2',
            '--ssh',
            '--regions', 'us-central1',
            '--project', 'idc-dev-etl',
            '--logging', 'gs://idc-dsub-app-logs',
            '--image', 'gcr.io/idc-dev-etl/tcia_cloner',
            '--mount', 'CLONE_TCIA={}'.format('gs://idc-dsub-clone-tcia'),
            '--env', 'TCIA_NAME="{}"'.format(tasks[task].split('\t')[0]),
            '--output', 'SERIES_STATISTICS={}'.format(series_statistics),
            '--output', 'OUTPUT_FILE={}'.format(output_file),
            '--command',"'" + 'python '+'"${CLONE_TCIA}"'+'/clone_collection.py -c '+'"${TCIA_NAME}"'+' -p 4 > '+'"${OUTPUT_FILE}"' + "'"]

 #       print(dsub_dict)
        dsub_string = ' '.join(dsub_dict)

 #       print(dsub_string)
 #       print(shlex.split(dsub_string))

        if procs == 0:
            run_dsub(tasks[task].split('\t')[0], dsub_string)
        else:
            # Enqueue the dsub command   
            print("Enqueuing {}\n".format(dsub_string), file=sys.stdout, flush=True)
            task_queue.put((tasks[task].split('\t')[0], dsub_string))

        time.sleep(2)
        task += 1

    # Now wait for processes to complete
    try:
        while task >first_task:
            results = done_queue.get()
            print("Completed {}".format(results), file=sys.stdout, flush=True)
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
    parser.add_argument('--processes', '-p', type=int, default=1)
    parser.add_argument('--initial', '-i', type=int, default=1)
    parser.add_argument('--tasks', '-t', type=int, default=1)
    parser.add_argument('--file', '-f', default='tasks.tsv')
    argz = parser.parse_args()
    print(argz)
    main(argz.processes, argz.initial, argz.tasks, argz.file)

