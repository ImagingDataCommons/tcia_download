#!/usr/bin/env
import argparse
import os
import sys

from BQ.gen_BQ_third_party_table import id_3rd_party_series

# Generate a list of series that came from some third party analysis; for all dev collections

parser =argparse.ArgumentParser()
# parser.add_argument('--dones_file', default="",
#                     help="Don't generate dones file")
parser.add_argument('--dones_file', default='{}/BQ/lists/third_party_series_dev.json'.format(os.environ['PWD']),
                    help="File in which to accumulate results")
parser.add_argument('--collections', default="all",
                    help="File containing list of IDC collection IDs or 'all' for all collections")

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
id_3rd_party_series(args)