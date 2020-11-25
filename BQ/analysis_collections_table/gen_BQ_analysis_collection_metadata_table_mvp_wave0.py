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
import sys
import os
from BQ.analysis_collections_table.gen_BQ_analysis_collection_metadata_table import gen_collections_table

parser =argparse.ArgumentParser()
parser.add_argument('--third_party_DOIs_file', default='{}/{}'.format(os.environ['PWD'], 'lists/third_party_series_mvp_wave0.json'),
                    help='Table of series/DOI pairs ')
parser.add_argument('--bqdataset_name', default='idc_tcia_mvp_wave0', help='BQ dataset name')
parser.add_argument('--bqtable_name', default='idc_tcia_analysis_collections_metadata', help='BQ table name')
parser.add_argument('--region', default='us', help='Dataset region')
parser.add_argument('--project', default='idc-dev-etl')

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
gen_collections_table(args)