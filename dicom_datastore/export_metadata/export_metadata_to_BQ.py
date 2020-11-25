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

# Export metadata from a DICOM store to BQ

import argparse
import sys
import os
import json
import shlex
import subprocess
import time
from subprocess import PIPE
from googleapiclient.errors import HttpError

from helpers.dicom_helpers import get_dataset, get_dicom_store, create_dicom_store, import_dicom_instance

def export_dicom_metadata(args):
    """Export data to a Google Cloud Storage bucket by copying
    it from the DICOM store."""
    # client = get_client()
    # dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
    #     args.project, args.region, args.dcmdataset_name
    # )
    # dicom_store_name = "{}/dicomStores/{}".format(args.dicom_store_parent, args.dcmdatastore_name)
    #
    # body = {"BigQueryDestination": {"tableURI": "gs://{}".format(table_uri), "force": False}}
    #

    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout,encoding='utf-8').strip()
    cmd = ['curl', '-X', 'POST',
        '-H', '"' + 'Authorization: Bearer {}'.format(bearer) + '"',
        '-H', '"Content-Type: application/json; charset=utf-8"',
        '--data', '"' + "{{'bigqueryDestination': {{'tableUri': 'bq://{}.{}.{}'}}}}".format(args.project, args.bqdataset, args.bqtable) +'"',
        "https://healthcare.googleapis.com/v1/projects/{}/locations/{}/datasets/{}/dicomStores/{}:export".format(
            args.project, args.region,args.dcmdataset_name, args.dcmdatastore_name)
        ]

    results = subprocess.run(cmd, stdout=PIPE, stderr=PIPE)

    operation_id = json.loads(str(results.stdout,encoding='utf-8'))['name'].split('/')[-1]

    while True:
        results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE,
                                 stderr=PIPE)
        bearer = str(results.stdout, encoding='utf-8').strip()
        cmd = ['curl', '-X', 'GET',
               '-H', '"' + 'Authorization: Bearer {}'.format(bearer) + '"',
               "https://healthcare.googleapis.com/v1/projects/{}/locations/{}/datasets/{}/operations/{}".format(
                   args.project, args.region, args.dcmdataset_name, operation_id)
               ]

        results = subprocess.run(cmd, stdout=PIPE, stderr=PIPE)
        details = json.loads(str(results.stdout,encoding='utf-8'))

        if 'done' in details and details['done']:
            if 'error' in details:
                print('Done with errorcode: {}, message: {}'.format(details['error']['code'], details['error']['message']))
            else:
                print('Done')
            break
        else:
            time.sleep(60)


def export_metadata(args):
    # try:
    #     dataset = get_dataset(args.SA, args.project, args.region, args.dcmdataset_name)
    # except HttpError:
    #     print("Can't access dataset")
    #     exit(-1)
    #
    # try:
    #     datastore = get_dicom_store(args.project, args.region, args.dcmdataset_name, args.dcmdatastore_name)
    # except HttpError:
    #     # Datastore doesn't exist. Create it
    #     datastore = create_dicom_store(args.project, args.region, args.dcmdataset_name, args.dcmdatastore_name)
    # pass

    try:
        start = time.time()
        response=export_dicom_metadata(args)
        finished = time.time()
        elapsed = finished - start
        print('Elapsed time: {}'.format(elapsed))

    except HttpError as e:
        err=json.loads(e.content)
        print('Error loading {}; code: {}, message: {}'.format(bucket.name, err['error']['code'], err['error']['message']))


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--region', '-r', default='us', help='Dataset region')
    parser.add_argument('--project', '-p', default='canceridc-data')
    parser.add_argument('--dcmdataset_name', '-d', default='idc_tcia', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', '-s', default='idc_tcia_mvp_wave0', help='DICOM datastore name')
    parser.add_argument('--bqdataset', default='idc_tcia', help="BQ dataset name")
    parser.add_argument('--bqtable', default='idc_tcia_dicom_metadata_mvp_wave0', help="BQ table name")
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    if not args.SA == '':
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    export_metadata(args)