#!/usr/bin/env python
# import re
from google.cloud import storage
import subprocess
import os
import sys
from collections import OrderedDict
import zipfile
import shutil
import pydicom
from random import shuffle
from multiprocessing import Process, Queue
from queue import Empty
import logging

sys.path.append(os.environ['CLONE_TCIA'])
from helpers.tcia_helpers import *

DICOM = os.environ['TMPDIR']
PROJECT = os.environ['PROJECT']
REF_PREFIX = os.environ['REF_PREFIX']
TRG_PREFIX = os.environ['TRG_PREFIX']


# Get information from GCS about the blobs in a series
def get_series_info(bucket_name, study, series, storage_client):
    # print('get_series_info args, bucket_name: {}, study: {}, series: {}, project: {}, storage_client: {}'.format(
    #     bucket_name, study, series, PROJECT, storage_client
    # ))
    series_info = {}
    try:
        if storage_client.bucket(bucket_name).exists():
            blobs = storage_client.bucket(bucket_name, user_project=PROJECT).list_blobs(
                prefix="dicom/{}/{}/".format(study, series))
            # print('Got series info args, bucket_name: {}, study: {}, series: {}, project: {}, storage_client: {}'.format(
            #     bucket_name, study, series, PROJECT, storage_client
            # ))
            series_info = {blob.name: blob.crc32c for blob in blobs}
    except:
        # The bucket probably exists but we don't have access to it
        pass
        # print("Bucket {} does not exist".format(bucket_name))
    finally:
        return series_info


def validate_series(target_bucket_name, reference_bucket_name, study, series, storage_client, validation):
    
    # # Get info on each of the blobs in the the reference collection
    # ref_blobs = storage_client.bucket(reference_bucket_name, user_project=PROJECT).list_blobs(prefix="dicom/{}/{}".format(study,series))
    #
    # ref_info = {blob.name: blob.crc32c for blob in ref_blobs}
    trg_info = get_series_info(target_bucket_name, study, series, storage_client)
    ref_info = get_series_info(reference_bucket_name, study, series, storage_client)

    if len(ref_info) == 0:
        print("\tValidation error on {}".format("dicom/{}/{}".format(study,series)), file=sys.stdout, flush=True)
        print("\tSeries {} not in reference collection".format("dicom/{}/{}".format(study,series)), file=sys.stdout, flush=True)
        validation['series not in reference collection'] = 1
        return validation

    if not len(trg_info) == len(ref_info):
        # print("Validation error on {}".format("dicom/{}/{}".format(study,series)), file=sys.stderr, flush=True)
        # print("Instance count target: {}, reference: {}".format(len(trg_info),len(ref_info)), file=sys.stderr, flush=True)
        print("\tValidation error on {}".format("dicom/{}/{}".format(study,series)), file=sys.stdout, flush=True)
        print("\tInstance count target: {}, reference: {}".format(len(trg_info),len(ref_info)), file=sys.stdout, flush=True)
        for ImageInstanceUID in trg_info:
            if not ImageInstanceUID in ref_info:
                print("\t\t{} in target, not in reference".format(ImageInstanceUID), file=sys.stdout, flush=True)
        for ImageInstanceUID in ref_info:
            if not ImageInstanceUID in trg_info:
                print("\t\t{} in reference, not in target".format(ImageInstanceUID), file=sys.stdout, flush=True)
        validation['unequal instance counts'] = 1
        return validation

    for ImageInstanceUID in trg_info:
        # Check if the two series have the same number of instances but different instance UIDs
        if not ImageInstanceUID in ref_info:
            # print("Validation error on {}".format(ImageInstanceUID), file=sys.stderr, flush=True)
            # print("target instance not in GCS", file=sys.stderr, flush=True)
            print("\tValidation error on {}".format(ImageInstanceUID), file=sys.stdout, flush=True)
            print("\tInstance not in reference", file=sys.stdout, flush=True)
            validation['series content differs'] = 1
        # Check if the crc32c of this instance is different in the target and reference
        if not trg_info[ImageInstanceUID] == ref_info[ImageInstanceUID]:
            # print("Validation error on {}".format(ImageInstanceUID), file=sys.stderr, flush=True)
            # print("target crc32c: {}, gcs public crc32c: {}".format(trg_info[ImageInstanceUID],ref_info[ImageInstanceUID]), file=sys.stderr, flush=True)
            print("\tValidation error on {}".format(ImageInstanceUID), file=sys.stdout, flush=True)
            print("\ttrg crc32c: {}, ref crc32c: {}".format(trg_info[ImageInstanceUID],ref_info[ImageInstanceUID]), file=sys.stdout, flush=True)
            # print("Instance count target: {}, reference: {}".format(len(trg_info), len(ref_info)), file=sys.stdout, flush=True)
            validation['crc32c mismatch'] = 1

    if validation['series content differs'] == 0 and validation['crc32c mismatch'] == 0:
        validation['validated'] = 1
    return validation

def download_series_from_TCIA(study, series, validation):
    MAX_RETRIES=3
    compressed = 0

    retry = 0
    while retry < MAX_RETRIES:
        # Get a zip of the instances in this series to a file and unzip it
        result = TCIA_API_request_to_file("{}/{}/{}.zip".format(DICOM, study, series),
                                          "getImage", parameters="SeriesInstanceUID={}".format(series))
        if result == -1:
            # print("TCIA getImage failed for {}/{}".format(study,series), file=sys.stderr, flush=True)
            print("\tTCIA getImage failed for {}/{}".format(study, series), file=sys.stdout, flush=True)
            validation['series download failed'] = 1
            return {'returncode': -1, 'compressed': 0, 'validation': validation}

        compressed += os.path.getsize("{}/{}/{}.zip".format(DICOM, study, series))

        try:
            with zipfile.ZipFile("{}/{}/{}.zip".format(DICOM, study, series)) as zip_ref:
                zip_ref.extractall("{}/{}/{}".format(DICOM, study, series))
            if retry > 0:
                print("\tGot valid zipfile for {}/{} on retry {}".format(study, series, retry), file=sys.stdout,
                      flush=True)
            validation['downloaded'] = 1
            return {'returncode': 0, 'compressed': compressed, 'validation': validation}
        except zipfile.BadZipFile:
            # print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
            print("\tZip extract failed for {}/{} with error BadZipFile on retry {}".format(study, series, retry),
                  file=sys.stdout,
                  flush=True)
            retry += 1
        except zipfile.LargeZipFile:
            # print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
            print("\tZip extract failed for {}/{} with error LargeZipFile on retry {}".format(study, series, retry),
                  file=sys.stdout, flush=True)
            retry += 1
        except:
            # print("Zip extract failed for {}/{}".format(study, series), file=sys.stderr, flush=True)
            print(
                "\tZip extract failed for {}/{} with error {},{},{} on retry {}".format(study, series, sys.exc_info()[0],
                                                                                      sys.exc_info()[1],
                                                                                      sys.exc_info()[2], retry),
                file=sys.stdout, flush=True)
            retry += 1

    if retry == MAX_RETRIES:
        # zip_path = "{}/{}/{}.zip".format(DICOM, study, series)
        # bad_zip_blob_name = "gs://{}/badzips/{}/{}.zip".format(target_bucket_name, study, series)
        # subprocess.run(["gsutil", "-m", "-q", "cp", zip_path, bad_zip_blob_name])

        print("\tFailed to get valid zipfile after {} retries for {}/{}".format(MAX_RETRIES, study, series),
              file=sys.stdout, flush=True)
        validation['zip extract failed'] = 1
        return {'returncode': -1, 'compressed': 0, 'validation': validation}


def copy_series(study, series, image_count, target_bucket_name, reference_bucket_name, storage_client, validate):

    validation = OrderedDict()
    validation['downloaded'] = 0              # Have not previously downloaded this series to target GCS
    validation['previously downloaded'] = 0     # This series already in target GCS
    validation['validated'] = 0                 # Validated this series against the series in reference GCS
    validation['collection not in reference GCS'] = 0 # No validated because reference GCS does not have this collection
    validation['series not in reference collection'] = 0     # reference GCS has this collection but not this series
    validation['unequal instance counts'] = 0   # Number of instances in target GCS unequal to number in reference GCS
    validation['crc32c mismatch'] = 0           # crc32c of an instance doesn't match crc32c of reference GCS instance
    validation['invalid zip'] = 0               # Number of files in zip file doesn't match getSeries ImageCount
    validation['series content differs'] = 0    # Download series and reference GCS have same number of instances but instance UID(s) differs
    validation['series download failed'] = 0    # Download failed after N tries
    validation['GCS upload failed'] = 0         # Upload of series to GCS failed
    validation['zip extract failed'] = 0        # Failed to obtain valid zip file
    validation['invalid DICOM'] = 0             # File was not valid DICOM format
    validation['other error'] = 0

    # print("copy_series args, study: {}, series: {}, image_count: {}, target_bucket_name: {} reference_bucket_name: {} storage_clients: {}, validate: {}".format(
    #     study, series, image_count, target_bucket_name, reference_bucket_name, storage_client, validate))

    # Put the downloaded results here
    os.mkdir("{}/{}/{}".format(DICOM, study, series))

    try:

        trg_info = get_series_info(target_bucket_name, study, series, storage_client)

        # If GCS doesn't already have all the instances in a series, get the entire series (again).
        if not len(trg_info) == image_count:


            results = download_series_from_TCIA(study, series, validation)
            # logging.debug('download_series_from_TCIA results: returncode: %s, compressed: %s, validation: %s',
            #             results['returncode'], results['compressed'], results['validation'] )
            # print('download_series_from_TCIA results: returncode: {}, compressed: {}, validation: {}'.format(
            #             results['returncode'], results['compressed'], results['validation'] ))
            if results['returncode'] == -1:
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':results['validation']}
            compressed = results['compressed']

            # Get a list of the files from the download
            dcms = [dcm for dcm in os.listdir("{}/{}/{}".format(DICOM, study, series))]

            # Ensure that the zip has the expected number of instances
            if not len(dcms) == image_count:
                print("\tInvalid zip file for {}/{}".format(study,series), file=sys.stdout, flush=True)
                validation['invalid zip'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}

            # logging.debug(("Series download successful"))
            # print("Series download successful")

            uncompressed = 0
            for dcm in dcms:
                uncompressed += os.path.getsize("{}/{}/{}/{}".format(DICOM, study,series,dcm))

            # TCIA file names are based on the position of the image in a scan. We need names of the form
            #   <studyUID>/<seriesUID>/<instanceUID>
            # So use pydicom to open each file to get its UID and remame it
            print("{}/{}: {} instances".format(study, series, len(os.listdir("{}/{}/{}".format(DICOM, study, series)))), file=sys.stdout, flush=True)
            # Replace the TCIA assigned file name
            try:
                for dcm in dcms:
                    SOPInstanceUID = pydicom.read_file("{}/{}/{}/{}".format(DICOM, study, series,dcm)).SOPInstanceUID
                    file_name = "{}/{}/{}/{}".format(DICOM, study, series, dcm)
                    blob_name = "{}/{}/{}/{}.dcm".format(DICOM, study, series, SOPInstanceUID)
                    os.renames(file_name, blob_name)
            except pydicom.errors.InvalidDicomError:
                print("\tInvalid DICOM file for {}/{}".format(study,series), file=sys.stdout, flush=True)
                validation['invalid DICOM'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
            # logging.debug(("Renamed all files"))
            # print("Renamed all files")

            # Delete the zip file before we copy to GCS so that it is not copied
            os.remove("{}/{}/{}.zip".format(DICOM, study, series))

            try:
                # Copy the series to GCS
                src = "{}/{}/{}/*".format(DICOM, study, series)
                url = "gs://{}/dicom/{}/{}/".format(target_bucket_name, study, series)
                subprocess.run(["gsutil", "-m", "-q", "cp", "-r", src, url])
            except:
                # print("Upload to GCS failed for {}/{}".format(study,series), file=sys.stderr, flush=True)
                print("\tUpload to GCS failed for {}/{}".format(study,series), file=sys.stdout, flush=True)
                validation['GCS upload failed'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
            # logging.debug(("Uploaded instances to GCS"))
            # print("Uploaded instances to GCS")

            if validate:
                validation = validate_series(target_bucket_name, reference_bucket_name, study, series, storage_client, validation)
            else:
                validation['collection not in reference GCS'] = 1
            # logging.debug(("Validation after download successful"))
            # print("Validation after download successful")

            return {'study':study, 'series':series, 'compressed':compressed, 'uncompressed':uncompressed, 'validation':validation}

        else:
            # We've already downloaded this series, but validate it against reference bucket again.
            validation['previously downloaded'] = 1
            if validate:
                validation = validate_series(target_bucket_name, reference_bucket_name, study, series, storage_client, validation)
            else:
                validation['collection not in reference GCS'] = 1
            # logging.debug(("Validation without download successful"))
            # print("Validation without download successful")

            # return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':PREVIOUSLY_DOWNLOADED}
            return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
    except:
        # print("Unexpected error: {}".format(sys.exc_info()[0]), file=sys.stderr, flush=True)
        print("\tError in copy_series: {},{},{}".format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
        validation['other error'] = 1
        return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
    finally:
        # Delete the series from disk
        shutil.rmtree("{}/{}/{}".format(DICOM, study, series), ignore_errors=True)


#
# Function run by worker processes
#
def worker(input, output, project, validate):
    # print('worker args, input: {}, output: {}, project: {}, validate: {}'.format(
    #     input, output, project, validate))
    storage_client = storage.Client(project=project)
    for args in iter(input.get, 'STOP'):
        result = copy_series(*args, storage_client, validate)
        output.put(result)

        
def copy_collection(tcia_name, num_processes, storage_client, project):

    # print('copy_collection args, tcia_name: {}, num_processes: {}, storage_client: {}, project: {}'.format(
    #     tcia_name, num_processes, storage_client, project))

    processes = []
    collection_name = tcia_name.replace(' ','_')
    target_bucket_name= '{}{}'.format(TRG_PREFIX, collection_name.lower().replace('_','-'))
    reference_bucket_name = target_bucket_name.replace(TRG_PREFIX, REF_PREFIX)

    # Collect statistics on each series
    series_statistics = []
    compressed = 0
    uncompressed = 0

    try:
        # Determine if the reference archive has this collection. If so, validate
        # against it.
        try:
            validate = storage_client.lookup_bucket(reference_bucket_name) != None
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
            print('\tError getting studies list',file=sys.stdout, flush=True)
            return (compressed, uncompressed, series_statistics)

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
            print('\tError getting studies list', file=sys.stdout, flush=True)
            return(compressed, uncompressed, series_statistics)


        if num_processes==0:
            # for series in sorted_seriess:
            for series in seriess:
                results = copy_series(series['StudyInstanceUID'],series['SeriesInstanceUID'], series['ImageCount'], target_bucket_name, reference_bucket_name,
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
                task_queue.put((series['StudyInstanceUID'],series['SeriesInstanceUID'], series['ImageCount'], target_bucket_name, reference_bucket_name))
                enqueued_series.append(series['SeriesInstanceUID'])

            # Collect the results for each series
            try:
                while not enqueued_series == []:
                    # Get results of each series. Timeout if waiting too long
                    results = done_queue.get(10*60)
                    # logging.debug('In copt_collection, got results %s from copy_series',results)
                    # print('In copy_collection, got results {} from copy_series'.format(results))
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
        print("\tUnexpected error in copy_collection. Terminating: {},{},{}".format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
        compressed = -1
        uncompressed = -1
        for process in processes:
            process.terminate()
            process.join()
        return (compressed, uncompressed, [])

