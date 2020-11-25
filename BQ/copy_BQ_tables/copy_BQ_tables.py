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
from google.cloud import bigquery

def copy_tables(args):

    client = bigquery.Client()

    with open(args.tables) as f:
        tables = f.readlines()
    for table in tables:
        if table[0] != '#':
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
    parser.add_argument('--tables', default='lists/copy_mvp_wave1_tables.txt',
                        help='Tables (source/destination) to copy')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    copy_tables(args)
