#!/usr/bin/env
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

# Validate that data returned when a series guid is resolved is correct.

from google.cloud import bigquery, storage
import argparse
import json
import os
import sys
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS
import requests

def get_collections(BQ_client, storage_client, args):
    aux = "`{}.{}.{}`".format(args.project, args.dataset, args.aux_table)
    sql = \
    """SELECT DISTINCT IDC_Webapp_CollectionID as collection
    FROM {aux}
    ORDER BY IDC_Webapp_CollectionID
    """.format(aux=aux)
    # Run a query that generates the manifest data
    # Results go into a temporary table
    results = query_BQ(BQ_client, args.dataset, 'tmp_collection_names', sql, \
                       write_disposition='WRITE_TRUNCATE')

    dst_uri = "gs://{}/tmp_collection_names.json".format(args.dst_bucket)
    result = export_BQ_to_GCS(BQ_client, args.dataset, 'tmp_collection_names', dst_uri, destination_format="JSON")

    # Get the list of collections
    collections = storage_client.bucket('tmp_work_whc').blob('tmp_collection_names.json').\
        download_as_string().decode().strip().split('\n')
    return collections[1:]

def get_guids_and_urls(BQ_client, storage_client, collection, args):
    aux = "`{}.{}.{}`".format(args.project, args.dataset, args.aux_table)
    sql = \
    """SELECT IDC_Webapp_CollectionID as collection, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID,  GCS_URL as url, CRDC_UUIDS.series as series_guid, CRDC_UUIDS.instance as instance_guid
    FROM {aux}
    WHERE \"{collection}\" = IDC_Webapp_CollectionID 
    """.format(aux=aux, collection=collection)
    # Run a query that generates the manifest data
    # Results go into a temporary table
    results = query_BQ(BQ_client, args.dataset, args.tmp_table, sql, \
                       write_disposition='WRITE_TRUNCATE')

    dst_uri = "gs://{}/instance_guids_and_urls_{}_*.json".format(args.dst_bucket, collection)
    result = export_BQ_to_GCS(BQ_client, args.dataset, args.tmp_table, dst_uri, destination_format="JSON")

    # Get the contents of the list of blobs of this collection
    guids_and_urls = []
    for blob in storage_client.bucket(args.dst_bucket).list_blobs(prefix="instance_guids_and_urls_{}".format(collection)):
        raw = blob.download_as_string().decode().strip().split('\n')
        less_raw = [row.split('\t') for row in raw[1:]]
        rows = [{
            'StudyInstanceUID': row[1],
            'SeriesInstanceUID': row[2],
            'SOPInstanceUID': row[3],
            'url': row[4],
            'series_guid': row[5],
            'instance_guid': row[6]} for row in less_raw]
        guids_and_urls.extend(rows)

    return guids_and_urls

def resolve_guid(guid):
    response = requests.get('https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/{}'.format(guid))
    return response.json()['access_methods'][0]['access_url']['url']


def resolve_guid_hierarchical(guid, instance_guids):
    response = requests.get('https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/{}'.format(guid))
    pass

def resolve_guid_expanded(guid, instance_guids):
    query_string = {"expand": "true"}
    response = requests.get('https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/{}'.format(guid), \
                            params=query_string)
    if len(instance_guids) == len(response.json()['contents']):
        for contentObject in response.json()['contents']:
            if not contentObject['id'] in instance_guids:
                print('Resolved content guid {} not in series guids'.format(contentObject['id']))
    else:
        print('GUID {} incorrect content length'.format(guid))

    pass

def validate_collection(BQ_client, storage_client, collection, args):
    print("Validating: {}".format(collection))
    instance_guids_and_urls = get_guids_and_urls(BQ_client, storage_client, collection, args)
    series_guids = set(row['series_guid'] for row in instance_guids_and_urls)
    for guid in series_guids:
        if guid != "":
            instance_guids = [row['instance_guid'] for row in instance_guids_and_urls if guid == row['series_guid']]
            results = resolve_guid_expanded(guid, instance_guids)
            results = resolve_guid_hierarchical(guid, instance_guids)

def validate_series_guids(args):
    BQ_client = bigquery.Client(project=args.project)
    storage_client = storage.Client(project=args.project)

    collections = get_collections(BQ_client, storage_client, args)
    for collection in collections:
        validate_collection(BQ_client, storage_client, collection, args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default='idc_tcia_mvp_wave1',
                        help="BQ dataset")
    parser.add_argument('--aux_table', default='idc_tcia_auxilliary_metadata',
                        help="Name of auxilliary_metadata table")
    parser.add_argument('--tmp_table', default='tmp_urls_and_guids',
                        help="Table to temporarily hold query results")
    parser.add_argument('--dst_bucket', default='tmp_work_whc')
    parser.add_argument('--project', default="idc-dev-etl")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    result = validate_series_guids(args)