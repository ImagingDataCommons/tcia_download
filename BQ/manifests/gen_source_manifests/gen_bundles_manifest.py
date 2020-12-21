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


def gen_bundles_manifest(args):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=args.project)

    with open(args.sql_file) as f:
        sql = f.read().format(project=args.project, dataset=args.dataset, aux=args.aux_table)

    query_job = client.query(sql)  # Make an API request.

    series_lengths = {}

    n=0
    with open(args.csv_file, 'w') as f:
        print("bundle_names, ids", file=f )
        for row in query_job:
            if 'series' in row['bundle_names']:
                # It's a series
                series_lengths[row['bundle_names']] = len(row['ids'])
                if len(row['ids']) <= args.max_instances:
                    # Index this series
                    print("{},{}".format(json.dumps(row['bundle_names']).replace('"',''),json.dumps(row['ids']).replace('"','').replace(',','')), file=f)
                else:
                    print("Series {} too long: {}".format(row['bundle_names'], len(row['ids'])))
            else:
                # It's a study
                study_length = sum([series_lengths[series] for series in row['ids']])
                if study_length <= args.max_instances:
                    # Index this study
                    print("{},{}".format(json.dumps(row['bundle_names']).replace('"',''),json.dumps(row['ids']).replace('"','').replace(',','')), file=f)
                else:
                    n += 1
                    print("{}: Study {} too long: {}".format(n,row['bundle_names'], study_length))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sql_file', default='{}/sql/gen_IndexD_bundle_manifest_with_decorated_names.sql'.format(os.environ['PWD']),
                        help="File containing SQL for this query")
    parser.add_argument('--csv_file', default='{}/tables/idc_mvp_wave1_bundle_manifest.csv'.format(os.environ['PWD']),
                        help="CSV file in which to save results")
    parser.add_argument('--dataset', default='whc_dev',
                        help="BQ dataset")
    parser.add_argument('--aux_table', default='idc_tcia_auxilliary_metadata', \
                        help="Name of auxilliary_metadata table")
    parser.add_argument('--max_instances', default=3000, type=int, help="Maximum number of instances in a series or study")
    parser.add_argument('--project', default="idc-dev-etl")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    result = gen_bundles_manifest(args)