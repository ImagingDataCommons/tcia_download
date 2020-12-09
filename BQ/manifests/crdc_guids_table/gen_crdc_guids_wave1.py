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

# Create a crdc_guids BQ table from CRDC/DCF output manifests. The table lists
# study, series, and instance GUIDs for each DICOM instance

import argparse
import sys
from BQ.manifests.crdc_guids_table.gen_crdc_guids import join_guids_to_aux_table

parser = argparse.ArgumentParser()
parser.add_argument('--manifest_bucket', default='indexd_manifests', \
                    help='GCS "folder" containing manifests generated by crdc')
parser.add_argument('--bqdataset_name', default='idc_tcia_mvp_wave1', help='BQ dataset name')
parser.add_argument('--crdc_guids_table', default='crdc_guids', help='BQ table name')
parser.add_argument('--temp_blob_manifest_table', default='crdc_blob_manifest', \
                    help='Temporary BQ table holding crdc blob manifests')
parser.add_argument('--temp_series_bundle_manifest_table', default='crdc_series_bundle_manifest', \
                    help='Temporary BQ table holding crdc series bundle manifests')
parser.add_argument('--temp_study_bundle_manifest_table', default='crdc_study_bundle_manifest', \
                    help='Temporary BQ table holding crdc study bundle manifests')
parser.add_argument('--dicom_metadata_table', default='idc_tcia_dicom_metadata', \
                    help='DICOM metadata table')
parser.add_argument('--aux_table', default='idc_tcia_auxilliary_metadata', \
                    help='BQ auxilliary_metadata table name')
parser.add_argument('--gen_crdc_guids_table_sql_file', default='sql/gen_crdc_guids_table.sql', \
                    help='SQL for building crdc guids table')
parser.add_argument('--join_guids_to_aux_sql_file', default='./sql/join_auxilliary_table_to_crdc_guids_table.sql', \
                    help='SQL for joining guids table to aux table')
parser.add_argument('--region', default='us', help='Dataset region')
parser.add_argument('--project', default='idc-dev-etl')

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
results = join_guids_to_aux_table(args)