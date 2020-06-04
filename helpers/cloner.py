#!/usr/bin/env python
# import re
from google.cloud import storage
import subprocess
import os,sys
from collections import OrderedDict
import zipfile
import shutil
import pydicom
from random import shuffle
from multiprocessing import Process, Queue, current_process, freeze_support
from queue import Empty
import re

sys.path.append(os.environ['CLONE_TCIA'])

from helpers.tcia_helpers import *

DICOM = os.environ['TMPDIR']
print('TMPDIR: {}'.format(os.environ['TMPDIR']))
print('DICOM: {}'.format(DICOM))
#DICOM = '/mnt/data/workingdir'

def validate_series(idcs, chc_bucket_name, study, series, storage_client):                   
    
    chc_blobs = storage_client.bucket(chc_bucket_name,user_project='idc-dev-etl').list_blobs(prefix="dicom/{}/{}".format(study,series))
    
    chcs = {blob.name:blob.crc32c for blob in chc_blobs}

    if len(chcs) == 0:
        return 'series not in chc collection'
    
    try:
        assert len(idcs) == len(chcs)
    except:
        # print("Validation error on {}".format("dicom/{}/{}".format(study,series)), file=sys.stderr, flush=True)
        # print("Instance count idc: {}, chc: {}".format(len(idcs),len(chcs)), file=sys.stderr, flush=True)
        print("Validation error on {}".format("dicom/{}/{}".format(study,series)), file=sys.stdout, flush=True)
        print("Instance count idc: {}, chc: {}".format(len(idcs),len(chcs)), file=sys.stdout, flush=True)
        return 'unequal instance counts'
    
    for ImageInstanceUID in idcs:
        try:
            assert idcs[ImageInstanceUID] == chcs[ImageInstanceUID]
        except:
            if ImageInstanceUID in chcs:
                # print("Validation error on {}".format(ImageInstanceUID), file=sys.stderr, flush=True)
                # print("idc crc32c: {}, gcs public crc32c: {}".format(idcs[ImageInstanceUID],chcs[ImageInstanceUID]), file=sys.stderr, flush=True)
                print("Validation error on {}".format("dicom/{}/{}".format(study, series)), file=sys.stdout, flush=True)
                print("Instance count idc: {}, chc: {}".format(len(idcs), len(chcs)), file=sys.stdout, flush=True)
                return 'crc32c mismatch'
            else:
                # print("Validation error on {}".format(ImageInstanceUID), file=sys.stderr, flush=True)
                # print("idc instance not in GCS", file=sys.stderr, flush=True)
                print("Validation error on {}".format(ImageInstanceUID), file=sys.stdout, flush=True)
                print("idc instance not in GCS", file=sys.stdout, flush=True)
                return 'series content differs'
                            
    return 'validated'


# Get information from GCS about the blobs in a series
def get_idcs(idc_bucket_name, study, series, storage_client):
    idc_blobs = storage_client.bucket(idc_bucket_name,user_project='idc-dev-etl').list_blobs(prefix="dicom/{}/{}".format(study,series))
    idcs = {blob.name:blob.crc32c for blob in idc_blobs}
    return idcs

               
def copy_series(study, series, image_count, idc_bucket_name, chc_bucket_name, storage_client, validate):

    MAX_RETRIES=3
    compressed = 0
    uncompressed = 0

    validation = OrderedDict()
    validation['downloaded'] = 0              # Have not previously downloaded this series to IDC GCS
    validation['previously downloaded'] = 0     # THis series already in IDC GCS
    validation['validated'] = 0                 # Validated this series against the series in CHC GCS
    validation['collection not in CHC GCS'] = 0 # No validated because CHC GCS does not have this collection
    validation['series not in chc collection'] = 0     # CHC GCS has this collection but not this series
    validation['unequal instance counts'] = 0   # Number of instances in IDC GCS unequal to number in CHC GCS
    validation['crc32c mismatch'] = 0           # crc32c of an instance doesn't match crc32c of CHC GCS instance
    validation['invalid zip'] = 0               # Number of files in zip file doesn't match getSeries ImageCount
    validation['series content differs'] = 0    # Download series and CHC GCS have same number of instances but instance UID(s) differs
    validation['series download failed'] = 0    # Download failed after N tries
    validation['GCS upload failed'] = 0         # Upload of series to GCS failed
    validation['zip extract failed'] = 0        # Failed to obtain valid zip file
    validation['other error'] = 0

    try:
        idcs = get_idcs(idc_bucket_name, study, series, storage_client)

        # If GCS doesn't already have all the instances in a series, get the entire series (again).
        if not len(idcs) == image_count:

            validation['downloaded'] = 1
            # We don't have all the images yet
            os.mkdir("{}/{}/{}".format(DICOM, study, series))

            retry = 0
            while retry < MAX_RETRIES:
                # Get a zip of the instances in this series to a file and unzip it
                result = TCIA_API_request_to_file("{}/{}/{}.zip".format(DICOM, study, series),
                        "getImage", parameters="SeriesInstanceUID={}".format(series))
                if result == -1:
                    # print("TCIA getImage failed for {}/{}".format(study,series), file=sys.stderr, flush=True)
                    print("TCIA getImage failed for {}/{}".format(study,series), file=sys.stdout, flush=True)
                    validation['series download failed'] = 1
                    return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}

                compressed += os.path.getsize("{}/{}/{}.zip".format(DICOM, study, series))

                try:
                    with zipfile.ZipFile("{}/{}/{}.zip".format(DICOM, study, series)) as zip_ref:
                        zip_ref.extractall("{}/{}/{}".format(DICOM, study, series))
                    if retry > 0:
                        print("Got valid zipfile for {}/{} on retry {}".format(study,series,retry), file=sys.stdout, flush=True)
                    break
                except zipfile.BadZipFile:
                    # print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
                    print("Zip extract failed for {}/{} with error BadZipFile on retry {}".format(study, series, retry), file=sys.stdout,
                          flush=True)
                    retry += 1
                except zipfile.LargeZipFile:
                    # print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
                    print("Zip extract failed for {}/{} with error LargeZipFile on retry {}".format(study, series, retry), file=sys.stdout, flush=True)
                    retry +=1
                except:
                    # print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
                    print("Zip extract failed for {}/{} with error {},{},{} on retry {}".format(study, series, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2], retry), file=sys.stdout, flush=True)
                    retry +=1

            if retry == MAX_RETRIES:
                # zip_path = "{}/{}/{}.zip".format(DICOM, study, series)
                # bad_zip_blob_name = "gs://{}/badzips/{}/{}.zip".format(idc_bucket_name, study, series)
                # subprocess.run(["gsutil", "-m", "-q", "cp", zip_path, bad_zip_blob_name])

                print("Failed to get valid zipfile after {} retries for {}/{}".format(MAX_RETRIES, study, series), file=sys.stdout, flush=True)
                validation['zip extract failed'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}



            dcms = [dcm for dcm in os.listdir("{}/{}/{}".format(DICOM, study, series))]

            # Ensure that the zip has the expected number of instances
            if not len(dcms) == image_count:
                validation['invalid zip'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}

            for dcm in dcms:
                uncompressed += os.path.getsize("{}/{}/{}/{}".format(DICOM, study,series,dcm))


            print("    {}/{}: {} instances".format(study, series, len(os.listdir("{}/{}/{}".format(DICOM, study, series)))), file=sys.stdout, flush=True)
            # Replace the TCIA assigned file name
            for dcm in dcms:
                SOPInstanceUID = pydicom.read_file("{}/{}/{}/{}".format(DICOM, study, series,dcm)).SOPInstanceUID
                file_name = "{}/{}/{}/{}".format(DICOM, study, series, dcm)
                blob_name = "{}/{}/{}/{}.dcm".format(DICOM, study, series, SOPInstanceUID)
                os.renames(file_name, blob_name)

                # Validate the CRC32 of the files against the corresponding Google blob

            # Delete the zip file before we copy to GCS so that it is not copied
            os.remove("{}/{}/{}.zip".format(DICOM, study, series))

            try:
                # Copy the series to GCS
                src = "{}/{}/{}/*".format(DICOM, study, series)
                url = "gs://{}/dicom/{}/{}/".format(idc_bucket_name, study, series)
                subprocess.run(["gsutil", "-m", "-q", "cp", "-r", src, url])
            except:
                # print("Upload to GCS failed for {}/{}".format(study,series), file=sys.stderr, flush=True)
                print("Upload to GCS failed for {}/{}".format(study,series), file=sys.stdout, flush=True)
                validation['GCS upload failed'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}

            if validate:
                idcs = get_idcs(idc_bucket_name, study, series, storage_client)
                validated = validate_series(idcs, chc_bucket_name, study, series, storage_client)
                validation[validated] = 1
            else:
                validation['collection not in CHC GCS'] = 1

            return {'study':study, 'series':series, 'compressed':compressed, 'uncompressed':uncompressed, 'validation':validation}

        else:
            # We've already downloaded this series, but validate it against Google Healthcare again.
            validation['previously downloaded'] = 1
            if validate:
                idcs = get_idcs(idc_bucket_name, study, series, storage_client)
                validated = validate_series(idcs, chc_bucket_name, study, series, storage_client)
                validation[validated] = 1
            else:
                validation['collection not in CHC GCS'] = 1

            # return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':PREVIOUSLY_DOWNLOADED}
            return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
    except:
        # print("Unexpected error: {}".format(sys.exc_info()[0]), file=sys.stderr, flush=True)
        print("Error in copy_series: {},{},{}".format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
        validation['other error'] = 1
        return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
    finally:
        # Delete the series from disk
        shutil.rmtree("{}/{}/{}".format(DICOM, study, series), ignore_errors=True)


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

    try:
        # Determine if the Google TCIA archive has this collection. If so, validate
        # against it.
        try:
            validate = storage_client.lookup_bucket(chc_bucket_name) != None
            # except BadRequest:
        except:
            # Get here if there is an error due to "Bucket is requester pays bucket but no user project provided."
            validate = True

        if os.path.isdir('{}'.format(DICOM)):
            shutil.rmtree('{}'.format(DICOM))
        os.mkdir('{}'.format(DICOM))

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
            print('Error getting studies list',file=sys.stdout, flush=True)
            return(compressed, uncompressed, series_statistics)

        # Make a directory for each study so that we don't have to test for existence later
        for study in studies:
            os.mkdir('{}/{}'.format(DICOM, study['StudyInstanceUID']))
            
        try:
            # Get a list of the series in the collection
            seriess = TCIA_API_request('getSeries','Collection={}'.format(collection_name))
            # sorted_seriess = sorted(seriess, key = lambda i: i['ImageCount'],reverse=True)
            # Shuffle the list in the hope that this will even out the load on the TCIA server when there are multiple processes
            print('{} series'.format(len(seriess)), file=sys.stdout, flush=True)
            shuffle(seriess)
        except:
            # print('Error getting studies list', file=sys.stderr, flush=True)
            print('Error getting studies list', file=sys.stdout, flush=True)
            return(compressed, uncompressed, series_statistics)


        if num_processes==0:
            # for series in sorted_seriess:
            for series in seriess:
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

            # print("Queuing series to task queue", flush=True)
            # Queue the series to be processed by worker processors

            # for series in sorted_seriess:
            for series in seriess:
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

                # Tell child processes to stop
                for process in processes:
                    task_queue.put('STOP')


            except Empty as e:
                # print("Timeout in collection {}".format(tcia_name), file=sys.stderr, flush=True)
                print("Timeout in copy_collection {}".format(tcia_name), file=sys.stdout, flush=True)
                compressed = -1
                uncompressed = -1
                for process in processes:
                    process.terminate()
                    process.join()
                return(compressed, uncompressed, [])
        return(compressed, uncompressed, series_statistics)
    except:
        # print("Unexpected error: {}".format(sys.exc_info()[0]), file=sys.stderr, flush=True)
        print("Unexpected error in copy_collection. Terminating: {},{},{}".format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
        compressed = -1
        uncompressed = -1
        for process in processes:
            process.terminate()
            process.join()
        return (compressed, uncompressed, [])

