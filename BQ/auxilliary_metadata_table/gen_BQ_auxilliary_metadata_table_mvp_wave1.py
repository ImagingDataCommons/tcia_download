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
import sys
import os
from BQ.auxilliary_metadata_table.gen_BQ_auxillary_metadata_table import gen_aux_table

parser = argparse.ArgumentParser()
parser.add_argument('--collections', default='{}/{}'.format(os.environ['PWD'], '../../lists/idc_mvp_wave_1.txt'),
                    help='File listing collections to add to BQ table, or "all"')
parser.add_argument(('--join_metadata_to_aux_sql_file'), default='./sql/join_dicom_metadata_to_aux_metadata.sql')
parser.add_argument('--bucket_prefix', default='idc-tcia-')
parser.add_argument('--dataset', default='idc_tcia_mvp_wave1', help='BQ dataset name')
parser.add_argument('--dicom_metadata_table', default='idc_tcia_dicom_metadata', \
                    help='BQ dicom metadata table name')
parser.add_argument('--aux_src_table', default='idc_tcia_auxilliary_metadata_no_guids', \
                    help='BQ auxilliary_metadata table name')
parser.add_argument('--aux_dst_table', default='idc_tcia_auxilliary_metadata_no_guids_trimmed', \
                    help='BQ auxilliary_metadata table name')
parser.add_argument('--temp_table', default='idc_tcia_collection_metadata', \
                    help='Temporary table in which collection metadata is built before appending to auxilliary_metadata')
parser.add_argument('--region', default='us', help='GCS region')
parser.add_argument('--gcs_project', default='idc-dev-etl', help="Project of the GCS tables")
parser.add_argument('--bq_project', default='idc-dev-etl', help="Project of the BQ table to be created")
parser.add_argument('--version', default='1', help='IDC version')
parser.add_argument('--dones', default='./logs/gen_BQ_auxilliary_metadata_table_dones_wave1.txt', \
                    help='File of completed collections')
args = parser.parse_args()
print("{}".format(args), file=sys.stdout)
gen_aux_table(args)