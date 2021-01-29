#!/usr/bin/env python
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

# import re
from google.cloud import storage
import subprocess
import os
import sys
from collections import OrderedDict
import zipfile
import shutil
import pydicom
from pydicom.errors import InvalidDicomError
from random import shuffle
from multiprocessing import Process, Queue
from queue import Empty
import logging


# sys.path.append(os.environ['CLONE_TCIA'])
from utilities.tcia_helpers import TCIA_API_request_to_file, TCIA_API_request

DICOM = os.environ['TMPDIR']
# PROJECT = os.environ['PROJECT']
REF_PREFIX = os.environ['REF_PREFIX']
# TRG_PREFIX = os.environ['TRG_PREFIX']


# Get information from GCS about the blobs in a series
def get_series_info(project, bucket_name, study, series, storage_client):
    logging.debug('get_series_info args, bucket_name: %s, study: %s, series: %s, project: %s, storage_client: %s',
        bucket_name, study, series, project, storage_client)
    series_info = {}
    try:
        if storage_client.bucket(bucket_name).exists():
            blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs(
                prefix="dicom/{}/{}/".format(study, series))
            logging.debug('Got series info args, bucket_name: %s, study: %s, series: %s, project: %s, storage_client: %s',
                bucket_name, study, series, project, storage_client)
            series_info = {blob.name: blob.crc32c for blob in blobs}
    except:
        # The bucket probably exists but we don't have access to it
        pass
        logging.error("Bucket %s does not exist", bucket_name)
    finally:
        return series_info


def validate_series(project, target_bucket_name, reference_bucket_name, study, series, storage_client, validation):
    
    trg_info = get_series_info(project, target_bucket_name, study, series, storage_client)
    ref_info = get_series_info(project, reference_bucket_name, study, series, storage_client)

    if len(ref_info) == 0:
        logging.error("\tValidation error on dicom/%s/%s", study, series)
        logging.error("\tSeries dicom/%s/%s not in reference collection", study, series)
        validation['series not in reference collection'] = 1
        return validation

    if not len(trg_info) == len(ref_info):
        logging.error("\tValidation error on dicom/%s/%s", study, series)
        logging.error("\tInstance count target: %s, reference: %s", len(trg_info),len(ref_info))
        for ImageInstanceUID in trg_info:
            if not ImageInstanceUID in ref_info:
                logging.info("\t\t{} in target, not in reference", ImageInstanceUID)
        for ImageInstanceUID in ref_info:
            if not ImageInstanceUID in trg_info:
                logging.info("\t\t{}in reference, not in target", ImageInstanceUID)
        validation['unequal instance counts'] = 1
        return validation

    for ImageInstanceUID in trg_info:
        # Check if the two series have the same number of instances but different instance UIDs
        if not ImageInstanceUID in ref_info:
            logging.error("\tValidation error on %s", ImageInstanceUID)
            logging.error("\tInstance not in reference")
            validation['series content differs'] = 1
        # Check if the crc32c of this instance is different in the target and reference
        if not trg_info[ImageInstanceUID] == ref_info[ImageInstanceUID]:
            logging.error("\tValidation error on %s", ImageInstanceUID)
            logging.error("\ttrg crc32c: %s, ref crc32c: %s", trg_info[ImageInstanceUID],ref_info[ImageInstanceUID])
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
            logging.error("\tTCIA getImage failed for %s/%s", study, series)
            validation['series download failed'] = 1
            return {'returncode': -1, 'compressed': 0, 'validation': validation}

        compressed += os.path.getsize("{}/{}/{}.zip".format(DICOM, study, series))

        try:
            with zipfile.ZipFile("{}/{}/{}.zip".format(DICOM, study, series)) as zip_ref:
                zip_ref.extractall("{}/{}/{}".format(DICOM, study, series))
            if retry > 0:
                logging.info("\tGot valid zipfile for %s/%s on retry %s", study, series, retry)
            validation['downloaded'] = 1
            return {'returncode': 0, 'compressed': compressed, 'validation': validation}
        except zipfile.BadZipFile:
            logging.error("\tZip extract failed for %s/%s with error BadZipFile on retry %s", study, series, retry)
            retry += 1
        except zipfile.LargeZipFile:
            logging.error("\tZip extract failed for %s/%s with error LargeZipFile on retry %s", study, series, retry)
            retry += 1
        except:
            logging.error(
                "\tZip extract failed for %s/%s with error %s,%s,%s on retry %s", study, series, sys.exc_info()[0],
                                                                                      sys.exc_info()[1],
                                                                                      sys.exc_info()[2], retry)
            retry += 1

    if retry == MAX_RETRIES:
        logging.error("\tFailed to get valid zipfile after %s retries for %s/%s", MAX_RETRIES, study, series)
        validation['zip extract failed'] = 1
        return {'returncode': -1, 'compressed': 0, 'validation': validation}


def copy_series(project, study, series, image_count, target_bucket_name, reference_bucket_name, storage_client, \
                validate):

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

    logging.debug("copy_series args, study: %s, series: %s, image_count: %s, target_bucket_name: %s \ "
                  "reference_bucket_name: %s storage_clients: %s, validate: %s",
        study, series, image_count, target_bucket_name, reference_bucket_name, storage_client, validate)

    # Put the downloaded results here
    os.mkdir("{}/{}/{}".format(DICOM, study, series))

    try:

        trg_info = get_series_info(project, target_bucket_name, study, series, storage_client)

        # If GCS doesn't already have all the instances in a series, get the entire series (again).
        if not len(trg_info) == image_count:


            results = download_series_from_TCIA(study, series, validation)
            logging.debug('download_series_from_TCIA results: returncode: %s, compressed: %s, validation: %s',
                        results['returncode'], results['compressed'], results['validation'] )
            if results['returncode'] == -1:
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':results['validation']}
            compressed = results['compressed']

            # Get a list of the files from the download
            dcms = [dcm for dcm in os.listdir("{}/{}/{}".format(DICOM, study, series))]

            # Ensure that the zip has the expected number of instances
            if not len(dcms) == image_count:
                logging.error("\tInvalid zip file for %s/%s", study, series)
                validation['invalid zip'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}

            logging.debug(("Series download successful"))

            uncompressed = 0
            for dcm in dcms:
                uncompressed += os.path.getsize("{}/{}/{}/{}".format(DICOM, study,series,dcm))

            # TCIA file names are based on the position of the image in a scan. We want names of the form
            #   <studyUID>/<seriesUID>/<instanceUID>
            # So use pydicom to open each file to get its UID and remame it
            num_instances = len(os.listdir("{}/{}/{}".format(DICOM, study, series)))
            logging.info("%s/%s: %s instances", study, series, num_instances)
            # Replace the TCIA assigned file name
            try:
                for dcm in dcms:
                    SOPInstanceUID = pydicom.read_file("{}/{}/{}/{}".format(DICOM, study, series,dcm)).SOPInstanceUID
                    file_name = "{}/{}/{}/{}".format(DICOM, study, series, dcm)
                    blob_name = "{}/{}/{}/{}.dcm".format(DICOM, study, series, SOPInstanceUID)
                    os.renames(file_name, blob_name)
            except InvalidDicomError:
                logging.error("\tInvalid DICOM file for %s/%s", study, series)
                validation['invalid DICOM'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
            logging.debug(("Renamed all files"))

            # Delete the zip file before we copy to GCS so that it is not copied
            os.remove("{}/{}/{}.zip".format(DICOM, study, series))

            try:
                # Copy the series to GCS
                src = "{}/{}/{}/*".format(DICOM, study, series)
                url = "gs://{}/dicom/{}/{}/".format(target_bucket_name, study, series)
                subprocess.run(["gsutil", "-m", "-q", "cp", "-r", src, url])
            except:
                logging.error("\tUpload to GCS failed for %s/%s", study, series)
                validation['GCS upload failed'] = 1
                return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
            logging.debug(("Uploaded instances to GCS"))

            if validate:
                validation = validate_series(project, target_bucket_name, reference_bucket_name, study, series, \
                                             storage_client, validation)
            else:
                validation['collection not in reference GCS'] = 1
            logging.debug(("Validation after download successful"))
            # print("Validation after download successful")

            return {'study':study, 'series':series, 'compressed':compressed, 'uncompressed':uncompressed, 'validation':validation}

        else:
            # We've already downloaded this series, but validate it against reference bucket again.
            validation['previously downloaded'] = 1
            if validate:
                validation = validate_series(project, target_bucket_name, reference_bucket_name, study, series, \
                                             storage_client, validation)
            else:
                validation['collection not in reference GCS'] = 1
            logging.debug(("Validation without download successful"))

            return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
    except:
        logging.error("\tError in copy_series: %s,%s,%s", sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2])
        validation['other error'] = 1
        return {'study':study, 'series':series, 'compressed':0, 'uncompressed':0, 'validation':validation}
    finally:
        # Delete the series from disk
        shutil.rmtree("{}/{}/{}".format(DICOM, study, series), ignore_errors=True)


#
# Function run by worker processes
#
def worker(input, output, project, validate):
    logging.debug('worker args, input: %s, output: %s, project: %s, validate: %s', input, output, project, validate)
    storage_client = storage.Client(project=project)
    for args in iter(input.get, 'STOP'):
        result = copy_series(project, *args, storage_client, validate)
        output.put(result)

        
def copy_collection(tcia_collection_id, num_processes, storage_client, project, dst_prefix):
    logging.debug('copy_collection args, tcia_name: %s, num_processes: %s, storage_client: %s, project: %s',
                  tcia_collection_id, num_processes, storage_client, project)

    processes = []
    collection_name = tcia_collection_id.replace(' ', '_')
    target_bucket_name= '{}{}'.format(dst_prefix, collection_name.lower().replace('_','-'))
    if REF_PREFIX != "":
        reference_bucket_name = target_bucket_name.replace(dst_prefix, REF_PREFIX)
    else:
        reference_bucket_name = ""

    # Collect statistics on each series
    series_statistics = []
    compressed = 0
    uncompressed = 0

    try:
        # Determine if the reference archive has this collection. If so, validate against it.
        try:
            if reference_bucket_name != "":
                validate = storage_client.lookup_bucket(reference_bucket_name) != None
            else:
                validate = False
        # except BadRequest:
        except:
            # Get here if there is an error due to "Bucket is requester pays bucket but no user project provided."
            validate = True

        # Create a local working directory
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
            logging.info('%s studies', len(studies))
        except:
            logging.error('\tError getting studies list')
            return (compressed, uncompressed, series_statistics)

        # Make a directory for each study so that we don't have to test for existence later
        for study in studies:
            os.mkdir('{}/{}'.format(DICOM, study['StudyInstanceUID']))
            
        try:
            # Get a list of the series in the collection
            seriess = TCIA_API_request('getSeries','Collection={}'.format(collection_name))
            # sorted_seriess = sorted(seriess, key = lambda i: i['ImageCount'],reverse=True)
            # Shuffle the list in the hope that this will even out the load on the TCIA server when there are multiple processes
            logging.info('%s series', len(seriess))
            shuffle(seriess)
        except:
            logging.error('\tError getting studies list')
            return(compressed, uncompressed, series_statistics)


        if num_processes==0:
            # for series in sorted_seriess:
            for series in seriess:
                results = copy_series(project, series['StudyInstanceUID'],series['SeriesInstanceUID'], series['ImageCount'], target_bucket_name, reference_bucket_name,
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

            logging.debug("Queuing series to task queue")
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
                    logging.debug('In copy_collection, got results %s from copy_series',results)
                    enqueued_series.remove(results['series'])
                    compressed += results['compressed']
                    uncompressed += results['uncompressed']
                    series_statistics.append(results)

                # Tell child processes to stop
                for process in processes:
                    task_queue.put('STOP')

            except Empty as e:
                logging.error("Timeout in copy_collection %s", tcia_collection_id)
                compressed = -1
                uncompressed = -1
                for process in processes:
                    process.terminate()
                    process.join()
                return(compressed, uncompressed, [])

        shutil.rmtree('{}'.format(DICOM))
        return(compressed, uncompressed, series_statistics)
    except:
        logging.error("\tUnexpected error in copy_collection. Terminating: %s,%s,%s",
                      sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2])
        compressed = -1
        uncompressed = -1
        for process in processes:
            process.terminate()
            process.join()
        return (compressed, uncompressed, [])

