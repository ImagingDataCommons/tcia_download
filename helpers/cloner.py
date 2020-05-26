import re
from google.cloud import bigquery,storage
import subprocess

# from helpers.bq_helpers import *
# from helpers.tcia_helpers import *
import os,sys
sys.path.append(os.environ['CLONE_TCIA'])
# from helpers.cloner import copy_collection
from tcia_helpers import *

VALIDATED=1
NO_VALIDATION=0
UNEQUAL_INSTANCE_COUNT=-1
CRC32C_MISMATCH=-2

def validate_series(idc_bucket_name, chc_bucket_name, study, series, storage_client):                   
    
    idc_blobs = storage_client.bucket(idc_bucket_name,user_project='idc-dev-etl').list_blobs(prefix="dicom/{}/{}".format(study,series))
    chc_blobs = storage_client.bucket(chc_bucket_name,user_project='idc-dev-etl').list_blobs(prefix="dicom/{}/{}".format(study,series))
    
    idcs = {blob.name:blob.crc32c for blob in idc_blobs}
    chcs = {blob.name:blob.crc32c for blob in chc_blobs}
    
    try:
        assert len(idcs) == len(chcs)
    except:
        print("Validation error on {}".format("dicom/{}/{}".format(study,series)))
        print("Instance count idc: {}, chc: {}".format(len(idcs),len(chcs)))
        return UNEQUAL_INSTANCE_COUNT
    
    for k in idcs:
#         print('{}, {}, {}'.format(k,idcs[k], chcs[k]))
        try:
            assert idcs[k] == chcs[k]
        except:
            print("Validation error on {}".format(k))
            print("idc crc32c: {}, gcs public crc32c: {}".format(idcs[k],chcs[k]))
            return CRC32C_MISMATCH
                            
    return VALIDATED

from multiprocessing import Process, Queue, current_process, freeze_support
import re

               
def copy_series(study, series, idc_bucket_name, chc_bucket_name, storage_client, validate):
#    print('copy_series: study: {}, series: {}'.format(study, series))
#    print('copy_series: idc_bucket_name: {}, chc_bucket_name: {}'.format(idc_bucket_name, chc_bucket_name))

    validated = 0
    compressed = 0
    uncompressed = 0
    os.mkdir("/dicom/{}/{}".format(study,series))
    TCIA_API_request_to_file("/dicom/{}/{}.zip".format(study,series), 
                             "getImage", parameters="SeriesInstanceUID={}".format(series))
    compressed += os.path.getsize("/dicom/{}/{}.zip".format(study,series))

    with zipfile.ZipFile("/dicom/{}/{}.zip".format(study,series)) as zip_ref:
        zip_ref.extractall("/dicom/{}/{}".format(study,series))

    for dcm in os.listdir("/dicom/{}/{}".format(study,series)):
        uncompressed += os.path.getsize("/dicom/{}/{}/{}".format(study,series,dcm))

    o=0
    print("    {} instances".format(len(os.listdir("/dicom/{}/{}".format(study,series)))))        
    # Replace the TCIA assigned file name
    for dcm in os.listdir("/dicom/{}/{}".format(study,series)):
        SOPInstanceUID = pydicom.read_file("/dicom/{}/{}/{}".format(study,series,dcm)).SOPInstanceUID
#             print('    {}: {}'.format(o,SOPInstanceUID))
        file_name = "/dicom/{}/{}/{}".format(study,series,dcm) 
        blob_name = "/dicom/{}/{}/{}.dcm".format(study,series,SOPInstanceUID)
        os.renames(file_name, blob_name)
        
        # Validate the CRC32 of the files against the corresponding Google blob
        o += 1


    # Delete to zip file before we copy to GCS
    os.remove("/dicom/{}/{}.zip".format(study,series))

    # Copy the series to GCS
    src = "/dicom/{}/{}".format(study,series)
    url = "gs://{}/dicom/{}/{}".format(idc_bucket_name,study,series)
#     print(src, url)
#   !gsutil -m -q cp -r $src $url
    subprocess.run(["gsutil", "-m", "-q", "cp", "-r", src, url])
    
    if validate:
        validated = validate_series(idc_bucket_name, chc_bucket_name, study, series, storage_client)
    else:
        validated = NO_VALIDATION
        
    # Delete the series
    shutil.rmtree("/dicom/{}/{}".format(study,series))

    return {'study':study, 'series':series, 'compressed':compressed, 'uncompressed':uncompressed, 'validation':validated}


#
# Function run by worker processes
#

def worker(input, output, project, validate):
    storage_client = storage.Client(project=project)
    for args in iter(input.get, 'STOP'):
        result = copy_series(*args, storage_client, validate)
        output.put(result)


        
def copy_collection(tcia_name, num_processes, storage_client, project):

    processes = []
    collection_name = tcia_name.replace(' ','_')
    idc_bucket_name = 'idc-tcia-{}'.format(collection_name.lower().replace('_','-'))
    chc_bucket_name = idc_bucket_name.replace('idc', 'gcs-public-data--healthcare')

    # Collect statistics on each series
    series_statistics = [] 
    compressed = 0
    uncompressed = 0
    
    # Determine if the Google TCIA archive has this collection. If so, validate 
    # against it.
    try:
        validate = storage_client.lookup_bucket(chc_bucket_name) != None
    except BadRequest:
        # Get here if there is an error due to "Bucket is requester pays bucket but no user project provided."
        validate = True

    if os.path.isdir('/dicom'):
        shutil.rmtree('/dicom')
    os.mkdir('/dicom')  

    # Create queues
    task_queue = Queue()
    done_queue = Queue()
    
    # List of series enqueued
    enqueued_series = []

    try:
        # Get a list of the studies in the collection
        studies = TCIA_API_request('getPatientStudy','Collection={}'.format(collection_name))
        print('{} studies'.format(len(studies)))
    except:
        print('Error getting studies list',file=sys.stderr)
        raise RuntimeError ("Failed to get series list")

    # Make a directory for each study so that we don't have to test for existence later
    for study in studies:
        os.mkdir('/dicom/{}'.format(study['StudyInstanceUID']))
        
    try:
        # Get a list of the series in the collection
        seriess = TCIA_API_request('getSeries','Collection={}'.format(collection_name))
        sorted_seriess = sorted(seriess, key = lambda i: i['ImageCount'],reverse=True)
        print('{} series'.format(len(seriess)))
    except:
        print('Error getting studies list',file=sys.stderr)
        raise RuntimeError ("Failed to get studies list")


    if num_processes==1:
        pass
        for series in sorted_seriess:
            results = copy_series(series['StudyInstanceUID'],series['SeriesInstanceUID'], idc_bucket_name, chc_bucket_name,
                storage.Client(project=project), validate)
            compressed += results['compressed']
            uncompressed += results['uncompressed']
            series_statistics.append(results)
    else:
        # Start worker processes
        for process in range(num_processes):
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, project, validate)))
            processes[-1].start()

        # Queue the series to be processed by worker processors
        for series in sorted_seriess:
            task_queue.put((series['StudyInstanceUID'],series['SeriesInstanceUID'], idc_bucket_name, chc_bucket_name))
            enqueued_series.append(series['SeriesInstanceUID']) 
        
        try:
            while not enqueued_series == []:
                # Get results of each series. Timeout if waiting too long
                results = done_queue.get(10*60)
                enqueued_series.remove(results['series'])
                compressed += results['compressed']
                series_statistics.append(results)
            '''
            for series in sorted_seriess:
                # Get results of each series. Timeout if waiting too long
                results = done_queue.get(10*60)
                compressed += results['compressed']
                uncompressed += results['uncompressed']
                series_statistics.append(results)
            '''
            print("Got results for all series",file=sys.stdout)

            # Tell child processes to stop
            for process in processes:
                task_queue.put('STOP')

            # Should not need to do this
#            for process in processes:
#                p.terminate()
#                p.join()
                
        except queue.Empty as e:
            print("Timeout in collection {}".format(tcia_name))
            compressed = -1
            uncompressed = -1
            for process in processes:
                p.terminate()
                p.join()
                return(compressed, uncompressed, [])

    return(compressed, uncompressed, series_statistics)

                        
