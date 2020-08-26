#!/usr/bin/env

# Load all collections (buckets) having some bucket name prefix into a DICOM server store

import argparse
import sys
import os
from dicom_datastore.load_collections_into_datastore import load_collections

parser =argparse.ArgumentParser()
parser.add_argument('--bucket_prefix', default='idc-tcia-1-')
parser.add_argument('--collections', default='all',
                    help='Collections to import into DICOM store')
parser.add_argument('--region', default='us-central1', help='Dataset region')
parser.add_argument('--gch_dataset_name', default='idc_tcia_dev', help='Dataset name')
parser.add_argument('--gch_dicomstore_name', default='idc_tcia', help='Datastore name')
parser.add_argument('--project', default='idc-dev-etl')
parser.add_argument('--thirdpartytable', default='idc.third_party_series')
parser.add_argument('--log', default='{}/{}'.format(os.environ['PWD'], 'logs/load_dicom_store_dev.log'))
parser.add_argument('--period', default=30, help="seconds to sleep between checking operation status")

# parser.add_argument('--SA', '-a',
#         default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
# parser.add_argument('--SA', default = '', help='Path to service accoumt key')
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
# if not args.SA == '':
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
load_collections(args)