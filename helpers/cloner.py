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
INVALID_ZIP=-3
SERIES_CONTENT_DIFFERS=-4
SERIES_DOWNLOAD_FAILED=-5
GCS_UPLOAD_FAILED=-6
PREVIOUSLY_DOWNLOADED=-7
ZIP_EXTRACT_FAILED=-8

def validate_series(idcs, chc_bucket_name, study, series, storage_client):                   
    
    chc_blobs = storage_client.bucket(chc_bucket_name,user_project='idc-dev-etl').list_blobs(prefix="dicom/{}/{}".format(study,series))
    
    chcs = {blob.name:blob.crc32c for blob in chc_blobs}
    
    try:
        assert len(idcs) == len(chcs)
    except:
        print("Validation error on {}".format("dicom/{}/{}".format(study,series)), file=sys.stderr, flush=True)
        print("Instance count idc: {}, chc: {}".format(len(idcs),len(chcs)), file=sys.stderr, flush=True)
        return UNEQUAL_INSTANCE_COUNT
    
    for k in idcs:
#         print('{}, {}, {}'.format(k,idcs[k], chcs[k]))
        try:
            assert idcs[k] == chcs[k]
        except:
            if k in chcs:
                print("Validation error on {}".format(k), file=sys.stderr, flush=True)
                print("idc crc32c: {}, gcs public crc32c: {}".format(idcs[k],chcs[k]), file=sys.stderr, flush=True)
                return CRC32C_MISMATCH
            else:
                print("Validation error on {}".format(k), file=sys.stderr, flush=True)
                print("idc instance not in GCS", file=sys.stderr, flush=True)
                return SERIES_CONTENT_DIFFERS
                            
    return VALIDATED

from multiprocessing import Process, Queue, current_process, freeze_support
import re

# Get information from GCS about the blobs in a series
def get_idcs(idc_bucket_name, study, series, storage_client):
    idc_blobs = storage_client.bucket(idc_bucket_name,user_project='idc-dev-etl').list_blobs(prefix="dicom/{}/{}".format(study,series))
    idcs = {blob.name:blob.crc32c for blob in idc_blobs}
    return idcs

               
def copy_series(study, series, image_count, idc_bucket_name, chc_bucket_name, storage_client, validate):
#    print('copy_series: study: {}, series: {}'.format(study, series))
#    print('copy_series: idc_bucket_name: {}, chc_bucket_name: {}'.format(idc_bucket_name, chc_bucket_name))

    MAX_RETRIES=3
    validated = 0
    compressed = 0
    uncompressed = 0

    try:
        idcs = get_idcs(idc_bucket_name, study, series, storage_client)

        # If GCS doesn't already have all the instances in a series, get the entire series again.
        if not len(idcs) == image_count:
            # We don't have all the images yet

            os.mkdir("/dicom/{}/{}".format(study,series))

            retry = 0
            while retry < MAX_RETRIES:
                # Get a zip of the instances in this series to a file and unzip it
                try:
                    TCIA_API_request_to_file("/dicom/{}/{}.zip".format(study,series),
                            "getImage", parameters="SeriesInstanceUID={}".format(series))
                except:
                    print("TCIA getImage failed for {}/{}".format(study,series), file=sys.stderr, flush=True)
                    return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':SERIES_DOWNLOAD_FAILED}

                compressed += os.path.getsize("/dicom/{}/{}.zip".format(study,series))

                try:
                    with zipfile.ZipFile("/dicom/{}/{}.zip".format(study,series)) as zip_ref:
                        zip_ref.extractall("/dicom/{}/{}".format(study,series))
                    break
                except:
                    print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
                    retry +=1
            if retry == MAX_RETRIES:
                print("Failed to get valid zipfile after {} reries".format(MAX_RETRIES), file=sys.stderr, flush=True)
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':ZIP_EXTRACT_FAILED}

            dcms = [dcm for dcm in os.listdir("/dicom/{}/{}".format(study,series))]

            # Ensure that the zip has the expected number of instances
            if not len(dcms) == image_count:
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':INVALID_ZIP}

            for dcm in dcms:
                uncompressed += os.path.getsize("/dicom/{}/{}/{}".format(study,series,dcm))


            print("    {} instances".format(len(os.listdir("/dicom/{}/{}".format(study,series)))), file=sys.stdout, flush=True)
            # Replace the TCIA assigned file name
            for dcm in dcms:
                SOPInstanceUID = pydicom.read_file("/dicom/{}/{}/{}".format(study,series,dcm)).SOPInstanceUID
        #             print('    {}: {}'.format(o,SOPInstanceUID))
                file_name = "/dicom/{}/{}/{}".format(study,series,dcm)
                blob_name = "/dicom/{}/{}/{}.dcm".format(study,series,SOPInstanceUID)
                os.renames(file_name, blob_name)

                # Validate the CRC32 of the files against the corresponding Google blob

            # Delete to zip file before we copy to GCS so that it is not copied
            os.remove("/dicom/{}/{}.zip".format(study,series))

            try:
                # Copy the series to GCS
                src = "/dicom/{}/{}/*".format(study,series)
                url = "gs://{}/dicom/{}/{}/".format(idc_bucket_name,study,series)
                subprocess.run(["gsutil", "-m", "-q", "cp", "-r", src, url])
            except:
                print("Upload to GCS failed for {}/{}".format(study,series), file=sys.stderr, flush=True)
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':GCS_UPLOAD_FAILED}

            if validate:
                idcs = get_idcs(idc_bucket_name, study, series, storage_client)
                validated = validate_series(idcs, chc_bucket_name, study, series, storage_client)
            else:
                validated = NO_VALIDATION

            # Delete the series from disk
            shutil.rmtree("/dicom/{}/{}".format(study,series))

            return {'study':study, 'series':series, 'compressed':compressed, 'uncompressed':uncompressed, 'validation':validated}
        else:
            # We've already downloaded this series
            return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':PREVIOUSLY_DOWNLOADED}
    except:
        print("Unexpected error: {}".format(sys.exec_info([0])), file=sys.stderr, flush=True)
        return(0, 0, [])



#
# Function run by worker processes
#

def worker(input, output, project, validate):
    storage_client = storage.Client(project=project)
    for args in iter(input.get, 'STOP'):
        result = copy_series(*args, storage_client, validate)
        output.put(result)


        
def copy_collection(tcia_name, num_processes, storage_client, project):

    try:
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
            print('{} studies'.format(len(studies)), file=sys.stdout, flush=True)
        except:
            print('Error getting studies list',file=sys.stderr, flush=True)
            return(compressed, uncompressed, series_statistics)

        # Make a directory for each study so that we don't have to test for existence later
        for study in studies:
            os.mkdir('/dicom/{}'.format(study['StudyInstanceUID']))
            
        try:
            # Get a list of the series in the collection
            seriess = TCIA_API_request('getSeries','Collection={}'.format(collection_name))
            sorted_seriess = sorted(seriess, key = lambda i: i['ImageCount'],reverse=True)
            print('{} series'.format(len(seriess)), file=sys.stdout, flush=True)
        except:
            print('Error getting studies list', file=sys.stderr, flush=True)
            return(compressed, uncompressed, series_statistics)


        if num_processes==0:
            pass
            for series in sorted_seriess:
                results = copy_series(series['StudyInstanceUID'],series['SeriesInstanceUID'], series['ImageCount'], idc_bucket_name, chc_bucket_name,
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
                task_queue.put((series['StudyInstanceUID'],series['SeriesInstanceUID'], series['ImageCount'], idc_bucket_name, chc_bucket_name))
                enqueued_series.append(series['SeriesInstanceUID']) 
            
            try:
                while not enqueued_series == []:
                    # Get results of each series. Timeout if waiting too long
                    results = done_queue.get(10*60)
                    enqueued_series.remove(results['series'])
                    compressed += results['compressed']
                    uncompressed += results['uncompressed']
                    series_statistics.append(results)
                '''
                for series in sorted_seriess:
                    # Get results of each series. Timeout if waiting too long
                    results = done_queue.get(10*60)
                    compressed += results['compressed']
                    uncompressed += results['uncompressed']
                    series_statistics.append(results)
                '''
                print("Got results for all series", file=sys.stdout, flush=True)

                # Tell child processes to stop
                for process in processes:
                    task_queue.put('STOP')

                # Should not need to do this
    #            for process in processes:
    #                p.terminate()
    #                p.join()
                    
            except queue.Empty as e:
                print("Timeout in collection {}".format(tcia_name), file=sys.stderr, flush=True)
                compressed = -1
                uncompressed = -1
                for process in processes:
                    p.terminate()
                    p.join()
                    return(compressed, uncompressed, [])

        return(compressed, uncompressed, series_statistics)
    except:
        print("Unexpected error: {}".format(sys.exec_info([0])), file=sys.stderr, flush=True)
        return(0, 0, [])
0