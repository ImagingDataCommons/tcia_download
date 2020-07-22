#!/usr/bin/env

# Export metadata from a DICOM store to BQ

import argparse
import sys
import os
from dicom_datastore.export_metadata_to_BQ import export_metadata

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', '-r', default='us-central1', help='Dataset region')
    parser.add_argument('--project', '-p', default='idc-dev-etl')
    parser.add_argument('--dcmdataset_name', '-d', default='idc_tcia_mvp_wave0', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', '-s', default='idc_tcia', help='DICOM datastore name')
    parser.add_argument('--bqdataset', default='idc_tcia_mvp_wave0', help="BQ dataset name")
    parser.add_argument('--bqtable', default='idc_tcia_dicom_metadata', help="BQ table name")
    # parser.add_argument('--SA', '-a',
    #         default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    # if not args.SA == '':
    #     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    export_metadata(args)