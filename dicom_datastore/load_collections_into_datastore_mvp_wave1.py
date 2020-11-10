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

# Load all collections (buckets) having some bucket name prefix into a DICOM server store

import argparse
import sys
import os
from dicom_datastore.load_collections_into_datastore import load_collections

parser =argparse.ArgumentParser()
parser.add_argument('--bucket_prefix', default='idc-tcia-1-')
parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'], 'lists/idc_mvp_wave_1.txt'),
                    help='Collections to import into DICOM store')
parser.add_argument('--region', default='us-central1', help='Dataset region')
parser.add_argument('--gch_dataset_name', default='idc_tcia_mvp_wave1', help='Dataset name')
parser.add_argument('--gch_dicomstore_name', default='idc_tcia', help='Datastore name')
parser.add_argument('--project', default='idc-dev-etl')
parser.add_argument('--thirdpartytable', default='idc_tcia_mvp_wave0.idc_tcia_third_party_series')
parser.add_argument('--log', default='{}/{}'.format(os.environ['PWD'], 'logs/load_dicom_store_mvp_wave1.log'))
parser.add_argument('--period', default=30, help="seconds to sleep between checking operation status")
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
load_collections(args)