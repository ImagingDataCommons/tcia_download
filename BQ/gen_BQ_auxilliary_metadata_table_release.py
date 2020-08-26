import argparse
import sys
import os
from BQ.gen_BQ_auxillary_metadata_table import gen_aux_table

parser =argparse.ArgumentParser()
parser.add_argument('--collections', default='/{}/{}'.format(os.environ['PWD'], '../lists/idc_mvp_wave_0.txt'),
                    help='File listing collections to add to BQ table, or "all"')
parser.add_argument('--bucket_prefix', default='idc-tcia-')
parser.add_argument('--gch_dataset_name', default='idc', help='DICOM dataset name')
parser.add_argument('--gch_datastore_name', default='idc', help='DICOM datastore name')
parser.add_argument('--bq_dataset_name', default='idc', help='BQ dataset name')
parser.add_argument('--bq_table_name', default='auxilliary_metadata', help='BQ table name')
parser.add_argument('--region', default='us', help='Dataset region')
parser.add_argument('--project', default='canceridc-data', help="Project of the GCS, BQ and GCH tables")
parser.add_argument('--schema', default='{}/helpers/etl_metadata_schema.py'.format(os.environ['PWD']))
parser.add_argument('--version', default='1', help='IDC version')
parser.add_argument('--dones', default='./logs/gen_BQ_auxilliary_metadata_table_dones_release.txt', help='File of completed collections')
parser.add_argument('--period', default=5, help="minutes to sleep between checking operation status")
# parser.add_argument('--SA', '-a',
#         default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
# parser.add_argument('--SA', default='', help='Path to service accoumt key')
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
# if not args.SA == '':
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
gen_aux_table(args)