import argparse
import os
import sys

from .gen_bundles_manifest import gen_bundles_manifest

parser = argparse.ArgumentParser()
parser.add_argument('--sql_file', default='{}/sql/gen_IndexD_bundle_manifest_with_decorated_names.sql'.format(os.environ['PWD']),
                    help="File containing SQL for this query")
parser.add_argument('--csv_file', default='{}/tables/idc_mvp_wave1_bundle_manifest.csv'.format(os.environ['PWD']),
                    help="CSV file in which to save results")
parser.add_argument('--dataset', default='idc_dev_mvp_wave1',
                    help="BQ dataset")
parser.add_argument('--aux_table', default='idc_tcia_auxilliary_metadata', \
                    help="Name of auxilliary_metadata table")
parser.add_argument('--max_instances', default=3000, type=int, help="Maximum number of instances in a series or study")
parser.add_argument('--project', default="idc-dev-etl")
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
result = gen_bundles_manifest(args)