
import argparse
import sys
import os
from dicom_datastore.load_collections_into_datastore import load_collections
from google.cloud import bigquery

def copy_tables(args):

    client = bigquery.Client()

    with open(args.tables) as f:
        tables = f.readlines()
    for table in tables:
        source_table_id = table.strip().split(',')[0].strip()
        destination_table_id = table.strip().split(',')[1].strip()

    # source_table_id = "idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_analysis_collections_metadata"
    #
    # destination_table_id = "canceridc-data.idc.analysis_collections_metadata"

        print('Copy {} to {}'.format(source_table_id, destination_table_id))

        job = client.copy_table(source_table_id, destination_table_id)
        job.result()  # Wait for the job to complete.


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tables', default='lists/tables.txt',
                        help='Tables (source/destination) to copy')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    copy_tables(args)
