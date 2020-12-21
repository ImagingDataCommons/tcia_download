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

# Create (temporary) tables of instance GUIDs, series GUIDs and study GUIDs from DCF output manifests.
# Generate a (temporary) crdc_guids table of instance, series and study GUIDs per SOPInstanceUID
# Join the crdc_guids table to auxilliary_metadata BQ table
# Delete the temporary tables

import argparse
import sys
import os
import json
import time
from BQ.manifests.add_guids_to_aux.schemas.crdc_manifest import crdc_blob_manifests_schema, \
    crdc_study_bundle_manifests_schema, crdc_series_bundle_manifests_schema
from BQ.manifests.add_guids_to_aux.schemas.crdc_guids import crdc_guids_schema
from google.cloud import bigquery, storage
from utilities.bq_helpers import load_BQ_from_json, delete_BQ_Table, create_BQ_table, load_BQ_from_uri, query_BQ


def load_bundle_manifests(storage_client,BQ_client, args):
    # Accumulate the raw manifests. Start with an empty table
    result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.temp_series_bundle_manifest_table)
    try:
        create_BQ_table(BQ_client, args.project, args.bqdataset_name, args.temp_series_bundle_manifest_table, crdc_series_bundle_manifests_schema)
    except:
        print("[ERROR] Failed to create ")
        return
    result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.temp_study_bundle_manifest_table)
    try:
        create_BQ_table(BQ_client, args.project, args.bqdataset_name, args.temp_study_bundle_manifest_table, crdc_study_bundle_manifests_schema)
    except:
        print("[ERROR] Failed to create ")
        return

    bucket = storage_client.bucket(args.manifest_bucket)
    all_manifests = list(bucket.list_blobs(prefix='dcf_output'))

    for manifest in all_manifests:
        if 'bundle' in manifest.name:
            print("Loading bundle manifest {}".format(manifest.name))
            # with open("temp_bundle_manifest", "wb") as f:
            #     manifest.download_to_file(f)
            # with open("temp_bundle_manifest") as f:
            #     manifest_data = f.readlines()
            manifest_data = [[row.split(',')[0], row.split(',')[-6]] for row in manifest.download_as_string().decode().split('\n')[1:-1]]
            # manifest_data = manifest.download_as_string().decode().split('\n')
            study_rows = []
            series_rows = []
            row_num = 0
            for manifest_row in manifest_data:
                row = []
                parts = manifest_row[0].split('_')
                if parts[0]=="":
                    break
                guid = manifest_row[1]
                if not guid:
                    print("Missing GUID for {}, row {} in manifest {}".format(manifest_row[0], row_num, manifest.name))
                    break
                if parts[1]=='series':
                    row = dict(
                        collection = parts[0],
                        SeriesInstanceUID = parts[2].split('/')[1] if parts[1]=='series' else "",
                        series_guid = guid if parts[1]=='series' else ""
                    )
                    series_rows.append(json.dumps(row))
                else:
                    row = dict(
                        collection = parts[0],
                        StudyInstanceUID = parts[2].split('/')[0],
                        study_guid = guid if parts[1]=='study' else "",
                    )
                    study_rows.append(json.dumps(row))
                row_num += 1

            json_series_rows = '\n'.join(series_rows)
            job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name,
                                    args.temp_series_bundle_manifest_table, \
                                    json_series_rows,
                                    crdc_series_bundle_manifests_schema)
            while not job.state == 'DONE':
                print('Status: {}'.format(job.state))
                time.sleep(args.period * 60)
            print("{}: Completed series bundle manifest upload \n".format(time.asctime()))

            json_study_rows='\n'.join(study_rows)
            job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name, args.temp_study_bundle_manifest_table, \
                                    json_study_rows,
                                    crdc_study_bundle_manifests_schema)
            while not job.state == 'DONE':
                print('Status: {}'.format(job.state))
                time.sleep(args.period * 60)
            print("{}: Completed study bundle manifest upload \n".format(time.asctime()))



def load_blob_manifests(storage_client, BQ_client, args):
    # Accumulate the raw manifests. Start with an empty table
    result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.temp_blob_manifest_table)
    try:
        create_BQ_table(BQ_client, args.project, args.bqdataset_name, args.temp_blob_manifest_table, crdc_blob_manifests_schema)
    except:
        print("[ERROR] Failed to create ")
        return

    if args.excluded_manifests:
        with open(args.excluded_manifests) as f:
            excludes = [manifest.strip() for manifest in f.readlines()]
    else:
        excludes = []

    bucket = storage_client.bucket(args.manifest_bucket)
    all_manifests = list(bucket.list_blobs(prefix='dcf_output'))

    for manifest in all_manifests:
        if not 'bundle' in manifest.name:
             if not manifest.name.split('/')[1] in excludes:
                print("Loading blob manifest {}".format(manifest.name))
                result = load_BQ_from_uri(BQ_client, args.bqdataset_name, args.temp_blob_manifest_table, manifest.public_url, crdc_blob_manifests_schema)
    print("{}: Completed blob bundle manifest upload \n".format(time.asctime()))


def gen_crdc_guids_table(storage_client, BQ_client, args):
    # Create temporary instance blob, series bundle and study bundle BQ tables
    load_blob_manifests(storage_client, BQ_client, args)
    load_bundle_manifests(storage_client, BQ_client, args)

    # Start with an empty guids BQ table
    result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.crdc_guids_table)
    try:
        create_BQ_table(BQ_client, args.project, args.bqdataset_name, args.crdc_guids_table, crdc_guids_schema)
    except:
        print("[ERROR] Failed to create ")
        return
    dest_table_id = "{}.{}.{}".format(args.project, args.bqdataset_name, args.crdc_guids_table)
    job_config = bigquery.QueryJobConfig(destination=dest_table_id)

    # Execute a query against the temp tables and load results into the guids table
    with open(args.gen_crdc_guids_table_sql_file) as f:
        query = f.read().format(project=args.project, dataset=args.bqdataset_name, blob_manifest=args.temp_blob_manifest_table, \
             dicom_metadata=args.dicom_metadata_table, series_bundle_manifest=args.temp_series_bundle_manifest_table, \
            study_bundle_manifest=args.temp_study_bundle_manifest_table)
    job = BQ_client.query(query, job_config=job_config)  # Make an API request.
    try:
        result = job.result()
        # result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.temp_blob_manifest_table)
        # result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.temp_series_bundle_manifest_table)
        # result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.temp_study_bundle_manifest_table)

    except Exception as e:
        print("[Error] Failed to create crdc_guids table: {}".format(e))

    print("{}: Completed guids_table creation \n".format(time.asctime()))


def join_guids_to_aux_table(args):
    storage_client = storage.Client(project=args.project)
    BQ_client = bigquery.Client(project=args.project)

    gen_crdc_guids_table(storage_client, BQ_client, args)

    # Now join the crdc_uuids table to the auxilliary_metadata table
    # Note that this will exclude any rows from auxilliary_metadata that
    # are not in crdc_guids. In particular, crdc_guids might not include
    # some 3rd party instances.
    aux = "{}.{}.{}".format(args.project, args.bqdataset_name, args.aux_src_table)
    guids = "{}.{}.{}".format(args.project, args.bqdataset_name, args.crdc_guids_table)
    dicom_metadata = "{}.{}.{}".format(args.project, args.bqdataset_name, args.dicom_metadata_table)
    # add_crdc_uuids(BQ_client, args)
    with open(args.join_guids_to_aux_sql_file) as f:
        sql = f.read().format(aux=aux, guids=guids)
    result = query_BQ(BQ_client, args.bqdataset_name, args.aux_dst_table, sql, 'WRITE_TRUNCATE')


    # result = delete_BQ_Table(BQ_client, args.project, args.bqdataset_name, args.crdc_guids_table)

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--manifest_bucket', default='indexd_manifests', \
                        help='GCS "folder" containing manifests generated by crdc')
    parser.add_argument('--bqdataset_name', default='whc_dev', help='BQ dataset name')
    parser.add_argument('--crdc_guids_table', default='crdc_guids', help='BQ table name')
    parser.add_argument('--temp_blob_manifest_table', default='crdc_blob_manifest', \
                         help='Temporary BQ table holding crdc blob manifests')
    parser.add_argument('--temp_series_bundle_manifest_table', default='crdc_series_bundle_manifest', \
                         help='Temporary BQ table holding crdc series bundle manifests')
    parser.add_argument('--temp_study_bundle_manifest_table', default='crdc_study_bundle_manifest', \
                         help='Temporary BQ table holding crdc study bundle manifests')
    parser.add_argument('--dicom_metadata_table', default='idc_tcia_dicom_metadata',  \
                         help='DICOM metadata table')
    parser.add_argument('--aux_src_table', default='idc_tcia_auxilliary_metadata_no_guids_trimmed', \
                        help='BQ auxilliary_metadata table name')
    parser.add_argument('--aux_dst_table', default='idc_tcia_auxilliary_metadata', \
                        help='BQ auxilliary_metadata table name')
    parser.add_argument('--gen_crdc_guids_table_sql_file', default='sql/gen_crdc_guids_table.sql',  \
                         help='SQL for building crdc guids table')
    parser.add_argument('--join_guids_to_aux_sql_file', default='./sql/join_auxilliary_table_to_crdc_guids_table.sql', \
                        help='SQL for joining guids table to aux table')
    parser.add_argument('--excluded_manifests', default = '',
                        help='List of manifests to exclude from process')
    parser.add_argument('--region', default='us', help='Dataset region')
    parser.add_argument('--project', default='idc-dev-etl')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    results = join_guids_to_aux_table(args)