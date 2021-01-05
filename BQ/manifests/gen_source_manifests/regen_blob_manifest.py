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

# Load data in some set of collections (stored in GCS buckets), and having some bucket name prefix, into a DICOM server store.
# The third party data of some collections is load conditionally. For this purpose, the BQ idc_tcia_third_party_series table
# is used to identify third party data, and, thus must have been previously generated.

from google.cloud import bigquery
import argparse
import json
import os
import sys

from utilities.bq_helpers import query_BQ, export_BQ_to_GCS
from utilities.bq_helpers import delete_BQ_Table, create_BQ_table

# This script generates an input DCF blob manifest that will updata DCF blobs for a specified collection.
# Such a manifest can be used to change the URLs of blobs.
def regen_blob_manifest(args):
    # Construct a BigQuery client object.
    BQ_client = bigquery.Client(project=args.project)

    aux = "`{}.{}.{}`".format(args.project, args.dataset, args.aux_table)

    with open(args.sql_file) as f:
        sql = f.read().format(collection=args.collection, aux=aux)

    # Run a query that generates the manifest data
    results = query_BQ(BQ_client, args.dataset, args.temp_table, sql, \
                       write_disposition='WRITE_TRUNCATE')

    results = export_BQ_to_GCS(BQ_client, args.dataset, args.temp_table, args.manifest_uri)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sql_file', default='{}/sql/gen_blob_manifest_from_aux.sql'.format(os.environ['PWD']),
                        help="File containing SQL for this query")
    parser.add_argument('--collection', default='nsclc_radiomics', \
                         help='Collection for which to regen blob manifest')
    parser.add_argument('--manifest_uri', default='gs://indexd_manifests/dcf_input/idc_mvp_wave1_indexd_manifest_v3.tsv'.format(os.environ['PWD']),
                        help="TSV file in which to save results")
    parser.add_argument('--dataset', default='idc_tcia_mvp_wave1',
                        help="BQ dataset")
    parser.add_argument('--aux_table', default='idc_tcia_auxilliary_metadata_with_nsclc', \
                        help="Name of auxilliary_metadata table fropm which to build table")
    parser.add_argument('--temp_table', default='tmp_manifest', \
                        help='Table in which to write query results')
    parser.add_argument('--max_instances', default=3000, type=int, help="Maximum number of instances in a series or study")
    parser.add_argument('--project', default="idc-dev-etl")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    result = regen_blob_manifest(args)