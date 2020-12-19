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

import argparse
import os
import sys

from .gen_bundles_manifest import gen_bundles_manifest

parser = argparse.ArgumentParser()
parser.add_argument('--sql_file', default='{}/sql/gen_IndexD_bundle_manifest_with_decorated_names.sql'.format(os.environ['PWD']),
                    help="File containing SQL for this query")
parser.add_argument('--csv_file', default='{}/tables/idc_mvp_wave1_bundle_manifest.csv'.format(os.environ['PWD']),
                    help="CSV file in which to save results")
parser.add_argument('--dataset', default='idc_dev_mvp_wave1',
                    help="BQ dataset")
parser.add_argument('--aux_table', default='idc_tcia_auxilliary_metadata', \
                    help="Name of auxilliary_metadata table")
parser.add_argument('--max_instances', default=3000, type=int, help="Maximum number of instances in a series or study")
parser.add_argument('--project', default="idc-dev-etl")
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
result = gen_bundles_manifest(args)