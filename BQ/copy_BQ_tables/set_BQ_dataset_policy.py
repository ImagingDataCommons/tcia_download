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

from google.cloud import bigquery
import argparse
import sys

# Set the BQ dataset sharing policy to READER for allAuthenticatedUsers

def set_BQ_dataset_policy(args):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    dataset_id = args.bq_dataset_id

    dataset = client.get_dataset(dataset_id)  # Make an API request.

    entry = bigquery.AccessEntry(
    role="READER",
    entity_type="specialGroup",
    entity_id="allAuthenticatedUsers",
    )

    entries = list(dataset.access_entries)
    entries.append(entry)
    dataset.access_entries = entries

    dataset = client.update_dataset(dataset, ["access_entries"])  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print(
    "Updated dataset '{}' with modified user permissions.".format(full_dataset_id)
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bq_dataset_id', default='canceridc-data.temp_dataset',
                        help='Tables (source/destination) to copy')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    set_BQ_dataset_policy(args)