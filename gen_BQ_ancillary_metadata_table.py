#!/usr/bin/env
import argparse
import sys
import os
import json
import pydicom
from googleapiclient.errors import HttpError
from google.cloud import storage
from google.cloud import bigquery
from helpers.bq_helpers import BQ_table_exists, create_BQ_table, load_BQ_from_json
from helpers.etl_metadata_schema import etl_metadata_schema
from helpers.tcia_helpers import get_TCIA_collections
from helpers.dicomweb_helpers import get_session
from helpers.gch_helpers import get_dataset
from helpers.dicom_helpers import get_dicom_store, create_dicom_store, import_dicom_instance

def get_collection_ID_dict():
        idc_collections = get_TCIA_collections()
        collection_ID_dict = {}
        for collection in idc_collections:
            tcia_collection_name = collection.replace(' ','-').replace('_','-'). lower()
            collection_ID_dict[tcia_collection_name] = collection
        return collection_ID_dict

def get_storage_client(project):
    storage_client = storage.Client(project=project)
    return storage_client

def get_collection_info(args, bucket_name,storage_client):
    # print('get_series_info args, bucket_name: {}, study: {}, series: {}, storage_client: {}'.format(
    #     bucket_name, study, series, storage_client
    # ))
    blobs = storage_client.bucket(bucket_name, user_project=args.project).list_blobs(
        prefix="dicom/")
    return blobs

def get_buckets(args):
    storage_client = get_storage_client(args.project)
    buckets = [bucket for bucket in storage_client.list_buckets(prefix=args.bucket_prefix, project=args.project)]
    buckets = [bucket for bucket in buckets if not '{}1'.format(args.bucket_prefix) in bucket.name]
    return buckets

def build_metadata(args, bucket_name, idc_collectionID, tcia_collectionID, storage_client):
#    collection_info = get_collection_info(args, bucket.name, storage_client)

    collection_info = get_collection_info(args, 'idc-tcia-lung-phantom', storage_client)
    metadata = ""
    for instance in collection_info:
        StudyInstanceUID = instance.id.split('/')[2]
        SeriesInstanceUID = instance.id.split('/')[3]
        SOPInstanceUID = instance.id.split('/')[4].split('.dcm')[0]
        generation = instance.id.split('/')[5]

        dicomweb_url_prefix = "https://healthcare.googleapis.com/v1/projects/{}/locations/{}/datasets/{}/dicomStores/{}/dicomWeb".format(
            args.project, args.region, args.dataset_name, args.datastore_name)
        row = {
            "SOPInstanceUID": SOPInstanceUID,
            "TCIA_CollectionID": tcia_collectionID,
            "IDC_CollectionID": idc_collectionID,
            "GCS_URLs": {
                "StudyInstanceUID": instance.public_url.rsplit('/',2)[0],
                "SeriesInstanceUID": instance.public_url.rsplit('/',1)[0],
                "SOPInstanceUID": '{}#{}'.format(instance.public_url,generation),
            },
            "DICOM_STORE_URLs": {
                "StudyInstanceUID":'{}/studies/{}'.format(
                    dicomweb_url_prefix, StudyInstanceUID),
                "SeriesInstanceUID": '{}/studies/{}/series/{}'.format(
                    dicomweb_url_prefix, StudyInstanceUID, SeriesInstanceUID),
                "SOPInstanceUID": '{}/studies/{}/series/{}/instances/{}'.format(
                    dicomweb_url_prefix, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID),
            },
            "INDEXD_GUIDSs": {
                "StudyInstanceUID": "",
                "SeriesInstanceUID": "",
                "SOPInstanceUID": "",
            },
            "IDC_Version": [
                args.version,
            ],
            "GCS_Generation": generation,
            "CRC32C_Hash": instance.crc32c,
            "MD5_Hash": instance.md5_hash,
            "Instance_Size": instance.size,
            "Time_Created": str(instance.time_created),
            "Time_Updated": str(instance.updated),
        }
        metadata = '{}{}\n'.format(metadata, json.dumps(row))

        #
        # row.append(SOPInstanceUID)                                                  # SOPInstanceUID
        # row.append(instance.public_url.rsplit('/',2)[0])                            # StudyInstanceUID_GCS_URL
        # row.append(instance.public_url.rsplit('/',1)[0])                            # SeriesInstanceUID_GCS_URL
        # row.append('{}#{}'.format(instance.public_url,generation))                  # SOPInstanceUID_GCS_URL
        # row.append('{}/studies/{}'.format(
        #     dicomweb_url_prefix, StudyInstanceUID))                                 # StudyInstanceUID_DICOM_STORE_URL
        # row.append('{}/studies/{}/series/{}'.format(
        #     dicomweb_url_prefix, StudyInstanceUID, SeriesInstanceUID))              # SeriesInstanceUID_DICOM_STORE_URL
        # row.append('{}/studies/{}/series/{}/instances/{}'.format(
        #     dicomweb_url_prefix, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID))  # SOPInstanceUID_DICOM_STORE_URL
        # row.append("")                                                              # StudyInstanceUID_IndexD_GUID
        # row.append("")                                                              # SeriesInstanceUID_IndexD_GUID
        # row.append("")                                                              # SOPInstanceUID_IndexD_GUID
        # row.append(args.version)                                                    # IDC_Version
        # row.append(generation)                                                      # GCS_generation
        # row.append(instance.crc32c)                                                 # CRC32C_Hash
        # row.append(instance.md5_hash)                                               # MD5_Hash
        # row.append(str(instance.size))                                              # Instance_Size
        # row.append(str(instance.time_created))                                      # Time_Created
        # row.append(str(instance.updated))                                           # Time_updated


    return metadata

def main(args):
    storage_client = get_storage_client(args.project)
    BQ_client = bigquery.Client()
    collection_ID_dict = get_collection_ID_dict()

    # Get the schema
    # with open(args.schema) as f:
    #     schema = json.load(f)

    if not BQ_table_exists(BQ_client, args.bqdataset_name, args.bqtable_name):
        try:
            table = create_BQ_table(BQ_client, args.bqdataset_name, args.bqtable_name, etl_metadata_schema)
        except:
            print("Error creating table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
                  file=sys.stdout, flush=True)
            print("Failed to create BQ table")
            exit()

    etl_data = []
    buckets = get_buckets(args)
    for bucket in buckets:
        idc_collectionID = bucket.name.split(args.bucket_prefix)[1]
        tcia_collectionID = collection_ID_dict[idc_collectionID]
        metadata = build_metadata(args, bucket.name, idc_collectionID, tcia_collectionID, storage_client)
        job = load_BQ_from_json(BQ_client, args.bqdataset_name, args.bqtable_name, metadata, etl_metadata_schema)
        pass

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/BillClifford/.config/gcloud/application_default_config.json'
    parser =argparse.ArgumentParser()
    parser.add_argument('--bucket_prefix', default='idc-tcia-')
    parser.add_argument('--dataset_name', default='idc-tcia', help='DICOM dataset name')
    parser.add_argument('--datastore_name', default='idc-tcia', help='DICOM datastore name')
    parser.add_argument('--bqdataset_name', default='idc_tcia', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='idc_tcia_ancillary_metadata', help='BQ table name')
    parser.add_argument('--region', default='us-central1', help='Dataset region')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--schema', default='{}/helpers/etl_metadata_schema.py'.format(os.environ['PWD']))
    parser.add_argument('--version', default='1', help='IDC version')
    parser.add_argument('--SA', default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args)