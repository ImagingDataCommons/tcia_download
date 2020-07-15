# tcia_download
Download TCIA collections to GCS

-Run 
-dsub_run.sh initiates dsub execution, driven by the tasks in task.tsv

-clone_collection.py does any cleanup necessary from previous runs, then calls helpers/cloner.py/copy_collection()

-copy_collection() is the main routine. It creates a pool of worker processes and then adds series UIDS into a queue

-Each worker process removes a series UID from the queue and calls copy series to perform the download and copy to GCS
