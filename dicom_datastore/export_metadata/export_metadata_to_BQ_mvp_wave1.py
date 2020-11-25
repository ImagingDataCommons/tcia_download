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
from dicom_datastore.export_metadata.export_metadata_to_BQ import export_metadata

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', '-r', default='us-central1', help='Dataset region')
    parser.add_argument('--project', '-p', default='idc-dev-etl')
    parser.add_argument('--dcmdataset_name', '-d', default='idc_tcia_mvp_wave1', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', '-s', default='idc_tcia', help='DICOM datastore name')
    parser.add_argument('--bqdataset', default='idc_tcia_mvp_wave1', help="BQ dataset name")
    parser.add_argument('--bqtable', default='idc_tcia_dicom_metadata', help="BQ table name")
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    export_metadata(args)