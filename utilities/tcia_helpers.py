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

import json
from subprocess import run, PIPE
import time, datetime
import logging
from io import BytesIO, StringIO

import requests
import backoff

# TCIA_URL = 'https://services.cancerimagingarchive.net/services/v4/TCIA/query'
TCIA_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v1'
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=3)
def get_url(url):  # , headers):
    results = requests.get(url)
    return results

def TCIA_API_request(endpoint, parameters=""):
    url = f'{TCIA_URL}/{endpoint}?{parameters}'
    results = get_url(url)
    results.raise_for_status()
    return results.json()


def TCIA_API_request_to_file(filename, endpoint, parameters=""):
    url = f'{TCIA_URL}/{endpoint}?{parameters}'
    results = get_url(url)
    results.raise_for_status()
    with open(filename, 'wb') as f:
        f.write(results.content)
    return 0


def get_TCIA_collections():
    results = TCIA_API_request('getCollectionValues')
    collections = [collection['Collection'] for collection in results]  
    return collections


def get_TCIA_patients_per_collection(collection):
    results = TCIA_API_request('getPatient','Collection={}'.format(collection))    
    patients = [patient['PatientID'] for patient in results]    
    return patients


def get_TCIA_studies_per_patient(collection, patient):
    results = TCIA_API_request('getPatientStudy','Collection={}&PatientID={}'.format(collection, patient))
    
    StudyInstanceUIDs = [StudyInstanceUID['StudyInstanceUID'] for StudyInstanceUID in results]
    
    return StudyInstanceUIDs

# print(get_TCIA_studies_per_patient('TCGA-READ','TCGA-BM-6198'))


def get_TCIA_series_per_study(collection, patient, study):
    results = TCIA_API_request('getSeries','Collection={}&PatientID={}&StudyInstanceUID={}'.format(collection, patient, study))
    SeriesInstanceUIDs = [SeriesInstanceUID['SeriesInstanceUID'] for SeriesInstanceUID in results]
    
    return SeriesInstanceUIDs

def get_TCIA_series_per_collection(collection):
    results = TCIA_API_request('getSeries')
    SeriesInstanceUIDs = [SeriesInstanceUID['SeriesInstanceUID'] for SeriesInstanceUID in results]

    return SeriesInstanceUIDs

def get_TCIA_series():
    results = TCIA_API_request('getSeries')
    
#     print(json.loads(buffer.getvalue().decode('iso-8859-1')))
#     return

    # We only need a few values 
    # We create a revision date field, filled with today's date (UTC +0), until TCIA returns a revision date 
    # in the response to getSeries
    today = datetime.date.today().isoformat()
    data = [{'CollectionID':result['Collection'],
          'StudyInstanceUID':result['StudyInstanceUID'],
          'SeriesInstanceUID':result['SeriesInstanceUID'],
          "SeriesInstanceUID_RevisionDate":today}
           for result in results]
    
    return data
# print(get_TCIA_series())

def get_TCIA_SOPInstanceUIDs(series):
    results = TCIA_API_request('getSOPInstanceUIDs', 'SeriesInstanceUID={}'.format(series))
    instances = [instance['SOPInstanceUID'] for instance in results]
    return instances

def get_TCIA_instance(series, instance):
    results = TCIA_API_request('getSingleImage', f'SeriesInstanceUID={series}&SOPInstanceUID={instance}')
    pass


def create_jsonlines_from_list(original):
    in_json = StringIO(json.dumps(original)) 
    result = [json.dumps(record) for record in json.load(in_json)]
    result = '\n'.join(result)
    return result


def get_collection_size(collection):
    size = 0
    serieses=TCIA_API_request('getSeries', parameters="Collection={}".format(collection.replace(' ','_')))
    print("{} series in {}".format(len(serieses), collection), flush=True)
    for aseries in serieses:
        seriesSize=TCIA_API_request('getSeriesSize', parameters="SeriesInstanceUID={}".format(aseries['SeriesInstanceUID']))
#             print(seriesSize)
        size += int(float(seriesSize[0]['TotalSizeInBytes']))
        print("{} {}\r".format(aseries['SeriesInstanceUID'], size),end="")
    return size


def get_collection_sizes_in_bytes():
    sizes = {}
    collections = get_TCIA_collections()
    collections.sort(reverse=True)
    for collection in collections:
        sizes[collection] = get_collection_size(collection)
    return sizes



def get_collection_instance_counts():
    collections = get_TCIA_collections()
    counts = {collection:0 for collection in collections}
    serieses=TCIA_API_request('getSeries')
    for aseries in serieses:
        counts[aseries['Collection']] += int(aseries['ImageCount'])
    sorted_counts = [(k, v) for k, v in sorted(counts.items(), key=lambda item: item[1])]
    return sorted_counts
# if __name__ == "__main__":
#     counts = get_collection_sizes()


def get_collection_descriptions():
    # Get access token for the guest account

    result = run([
        'curl',
        '-d',
        "username=nbia_guest&password=&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe&grant_type=password",
        '-X',
        'POST',
        '-k',
        "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
        ], stdout=PIPE, stderr=PIPE)
    access_token = json.loads(result.stdout.decode())['access_token']
    result = run([
        'curl',
        '-H',
        "Authorization:Bearer {}".format(access_token),
        '-k',
        'https://public.cancerimagingarchive.net/nbia-api/services/getCollectionDescriptions'
        ], stdout=PIPE, stderr=PIPE)
    descriptions = json.loads(result.stdout.decode())
    collection_descriptions = {description['collectionName']: description['description'] for description in descriptions}

    return collection_descriptions


if __name__ == '__main__':
    # instances = get_TCIA_SOPInstanceUIDs('1.3.6.1.4.1.14519.5.2.1.7695.4001.306204232344341694648035234440')
    # instance = get_TCIA_instance('1.3.6.1.4.1.14519.5.2.1.7695.4001.306204232344341694648035234440', instances[0])
    # result = TCIA_API_request('getImage', 'SeriesInstanceUID=1.3.6.1.4.1.14519.5.2.1.7695.4001.306204232344341694648035234440')
    collections = get_TCIA_collections()
    patients = get_TCIA_patients_per_collection(collections[0])
    studies = get_TCIA_studies_per_patient(collections[0], patients[0])
    series = get_TCIA_series_per_study(collections[0], patients[0], studies[0])
    result = TCIA_API_request_to_file(f'./dicom/{series[0]}.zip', 'getImage', f'SeriesInstanceUID={series[0]}')

    pass


