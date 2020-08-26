#!/usr/bin/env

# Load all collections (buckets) having some bucket name prefix into a DICOM server store

import argparse
import sys
import os
from dicom_datastore.load_collections_into_datastore import load_collections

parser =argparse.ArgumentParser()
parser.add_argument('--bucket_prefix', default='idc-tcia-1-')
parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'], 'lists/idc_mvp_wave_0.txt'),
                    help='Collections to import into DICOM store')
parser.add_argument('--region', default='us-central1', help='Dataset region')
parser.add_argument('--gch_dataset_name', default='idc_tcia_mvp_wave0', help='Dataset name')
parser.add_argument('--gch_dicomstore_name', default='idc_tcia', help='Datastore name')
parser.add_argument('--project', default='idc-dev-etl')
parser.add_argument('--thirdpartytable', default='idc_tcia_mvp_wave0.idc_tcia_third_party_series')
parser.add_argument('--log', default='{}/{}'.format(os.environ['PWD'], 'logs/load_dicom_store_mvp_wave0.log'))
parser.add_argument('--period', default=30, help="seconds to sleep between checking operation status")
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
load_collections(args)