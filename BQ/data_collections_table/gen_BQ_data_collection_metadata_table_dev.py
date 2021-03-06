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

import argparse
import os
import sys

from BQ.data_collections_table.gen_BQ_data_collection_metadata_table import gen_collections_table

parser =argparse.ArgumentParser()
parser.add_argument('--file', default='{}/{}'.format(os.environ['PWD'], '../collection_ids_file/lists/collection_ids_dev.json'),
                    help='Table to translate between collection IDs ')
parser.add_argument('--bqdataset_name', default='idc_tcia_dev', help='BQ dataset name')
parser.add_argument('--bqtable_name', default='idc_tcia_data_collections_metadata', help='BQ table name')
parser.add_argument('--region', default='us-central1', help='Dataset region')
parser.add_argument('--project', default='idc-dev-etl')

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
gen_collections_table(args)