#!/usr/bin/env

# Export metadata from a DICOM store to BQ

import argparse
import sys
import os
from dicom_datastore.export_metadata_to_BQ import export_metadata

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', '-r', default='us', help='Dataset region')
    parser.add_argument('--project', '-p', default='canceridc-data')
    parser.add_argument('--dcmdataset_name', '-d', default='idc', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', '-s', default='idc', help='DICOM datastore name')
    parser.add_argument('--bqdataset', default='idc', help="BQ dataset name")
    parser.add_argument('--bqtable', default='dicom_metadata', help="BQ table name")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    export_metadata(args)