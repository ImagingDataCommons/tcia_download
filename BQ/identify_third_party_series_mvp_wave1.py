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

import argparse
import os
import sys

from BQ.gen_BQ_seriesUID_to_third_party_table import id_3rd_party_series

# Generate a list of series that came from some third party analysis; limited to mvp_wave0

parser =argparse.ArgumentParser()
# parser.add_argument('--dones_file', default="",
#                     help="Don't generate dones file")
parser.add_argument('--dones_file', default='{}/lists/third_party_series_mvp_wave1.json'.format(os.environ['PWD']),
                    help="File in which to accumulate results")
parser.add_argument('--collections', default='{}/../lists/idc_mvp_wave_1.txt'.format(os.environ['PWD']),
                    help="File containing list of IDC collection IDs or 'all' for all collections")

args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
id_3rd_party_series(args)