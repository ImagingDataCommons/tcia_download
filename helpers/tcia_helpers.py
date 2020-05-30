import json
import sys
import pycurl
import zipfile
import os
import shutil
import pydicom
import time
import random
from io import BytesIO, StringIO

from google.cloud import bigquery,storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest

MAX_RETRIES=10

def TCIA_API_request(endpoint, parameters=""):  
    retry = 0
    buffer = BytesIO()
    c = pycurl.Curl()
    url = 'https://services.cancerimagingarchive.net/services/v3/TCIA/query/{}?{}'.format(endpoint,parameters)
    while retry < MAX_RETRIES:
        try:
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA,buffer)
            c.perform()
            data = buffer.getvalue().decode('iso-8859-1')
#            print('Raw TCIA data: {}'.format(data),file=sys.stderr)
            results = json.loads(data)
            c.close()
            return results
        except pycurl.error as e:
            print("Error {}; {} in TCIA_API_request".format(e[0],e[1]), file=sys.stderr, flush=True)
            rand = random.randint(1,10)
            print("Retrying in TCIA_API_request from {}".format(inspect.stack()[1]), file=sys.stderr, flush=True)
            print("Retry {}, sleeping {} seconds".format(retry, rand), file=sys.stderr, flush=True)
            time.sleep(rand)
            retry += 1
            
    c.close()
    print("TCIA_API_request failed in call from {}".format(inspect.stack()[1]), file=sys.stderr, flush=True)
    raise RuntimeError (inspect.stack()[0:2])


def TCIA_API_request_to_file(filename, endpoint, parameters=""):
    retry = 0
    c = pycurl.Curl()
    url = 'https://services.cancerimagingarchive.net/services/v3/TCIA/query/{}?{}'.format(endpoint,parameters)
    while retry < MAX_RETRIES:
        try:
            with open(filename, 'wb') as f:
                c.setopt(c.URL, url)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                return
        except pycurl.error as e:
            print("Error {}; {} in TCIA_API_request_to_file".format(e[0],e[1]), file=sys.stderr, flush=True)
            rand = random.randint(1,10)
            print("Retrying in TCIA_API_request_to_file from {}".format(inspect.stack()[1]), file=sys.stderr, flush=True)
            print("Retry {}, sleeping {} seconds".format(retry, rand), file=sys.stderr, flush=True)
            time.sleep(rand)
            retry += 1
            
    c.close()
    print("TCIA_API_request_to_file failed in call from {}".format(inspect.stack()[1]), file=sys.stderr, flush=True)
    raise RuntimeError (inspect.stack()[0:2])


def get_TCIA_collections():
    results = TCIA_API_request('getCollectionValues')
    collections = [collection['Collection'] for collection in results]  
    return collections

# collections = get_TCIA_collections()
# collections.sort()
# print(collections)

def get_TCIA_patients_per_collection(collection):
    results = TCIA_API_request('getPatient','Collection={}'.format(collection))    
    patients = [patient['PatientID'] for patient in results]    
    return patients

# print(get_TCIA_patients_per_collection('TCGA-READ'))
# print(get_TCIA_patients_per_collection('GBM-DSC-MRI-DRO'))
    

def get_TCIA_studies_per_patient(collection, patient):
    results = TCIA_API_request('getPatientStudy','Collection={}&PatientID={}'.format(collection, patient))
    
    StudyInstanceUIDs = [StudyInstanceUID['StudyInstanceUID'] for StudyInstanceUID in results]
    
    return StudyInstanceUIDs

# print(get_TCIA_studies_per_patient('TCGA-READ','TCGA-BM-6198'))


def get_TCIA_series_per_study(collection, patient, study):
    results = TCIA_API_request('getSeries','Collection={}&PatientID={}&StudyInstanceUID={}'.format(collection, patient, study))
    SeriesInstanceUIDs = [SeriesInstanceUID['SeriesInstanceUID'] for SeriesInstanceUID in results]
    
    return SeriesInstanceUIDs

# print(get_TCIA_series_per_study('TCGA-READ','TCGA-BM-6198', '1.3.6.1.4.1.14519.5.2.1.8421.4018.304030957341830836628192929917'))


def get_TCIA_series():
    results = TCIA_API_request('getSeries')
    
#     print(json.loads(buffer.getvalue().decode('iso-8859-1')))
#     return

    # We only need a few values 
    # We create a revision date field, filled with today's date (UTC +0), until TCIA returns a revision date 
    # in the response to getSeries
    today = date.today().isoformat()
    data = [{'CollectionID':result['Collection'],
          'StudyInstanceUID':result['StudyInstanceUID'],
          'SeriesInstanceUID':result['SeriesInstanceUID'],
          "SeriesInstanceUID_RevisionDate":today}
           for result in results]
    
    return data
# print(get_TCIA_series())

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

def get_collection_sizes():
    sizes = {}
    collections = get_TCIA_collections()
    collections.sort(reverse=True)
    for collection in collections:
        sizes[collection] = get_collection_size(collection)
    return sizes

# sizes = get_collection_sizes()
