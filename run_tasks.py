from multiprocessing import Process, Queue
import io
import subprocess
from subprocess import PIPE
from os.path import join
import os
import argparse
import shlex
import time

def run_dsub(tcia_name, dsub_string):
#    stream = os.popen(dsub_string)
#    output = stream.read()
    # shlex splits the command line correctly, so that subprocess.run will take it
    args = shlex.split(dsub_string)
    print("Starting {} at {}".format(dsub_string,time.asctime()))
    results = subprocess.run(args, stdout=PIPE, stderr=PIPE)
    return(tcia_name,results)


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
        processes.append(Process(target=worker, args=(task_queue, done_queue)).start())
        
    # Get the list of possible tasks
    with open(task_file) as f:
        tasks = f.read().splitlines()
        
    task = first_task
    
#     print(tasks)

    while task < first_task + total_tasks:      
        dsub_dict = [
            '/home/bcliffor/virtualEnv2/bin/dsub',
            '--provider', 'google-v2',
            '--regions', 'us-central1',
            '--project', 'idc-dev-etl',
            '--logging', 'gs://idc-dsub-app-logs',
            '--image', 'gcr.io/idc-dev-etl/tcia_cloner',
            '--mount', 'CLONE_TCIA={}'.format('gs://idc-dsub-clone-tcia'),
            '--env', 'TCIA_NAME="{}"'.format(tasks[task].split('\t')[0]),
            '--output', 'SERIES_STATISTICS={}'.format(tasks[task].split('\t')[1]),
            '--output', 'OUTPUT_FILE={}'.format(tasks[task].split('\t')[2]),
            '--command',"'" + 'python '+'"${CLONE_TCIA}"'+'/clone_collection.py -c '+'"${TCIA_NAME}"'+' -p 4 > '+'"${OUTPUT_FILE}"' + "'",
            '--wait']

 #       print(dsub_dict)
        dsub_string = ' '.join(dsub_dict)

 #       print(dsub_string)
 #       print(shlex.split(dsub_string))

        # Enqueue the dsub command   
        print("Enqueuing {}\n".format(dsub_string))   
        task_queue.put((tasks[task].split('\t')[0], dsub_string))
        task += 1

    # Now wait for processes to complete
    try:
        while total_tasks > 0:
            results = done_queue.get()
            print("Completed {}, results:".format(results[0]))
            print("{}\n".format(str(results[1].stderr)))
            total_tasks -= 1
        # Tell child processes stop
        for process in processes:
            task_queue.put('STOP')
    except:
        # Something went wrong. Presumable an error message was logged
        print("Killing processes")
        for process in processes:
            p.terminate()
            p.join()
            
          
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', '-p', type=int, default=1)
    parser.add_argument('--initial', '-i', type=int, default=1)
    parser.add_argument('--tasks', '-t', type=int, default=1)
    parser.add_argument('--file', '-f', default='tasks.tsv')
    argz = parser.parse_args()
    print(argz)
    main(argz.processes, argz.initial, argz.tasks, argz.file)

