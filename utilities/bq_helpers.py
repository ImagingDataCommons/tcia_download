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

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
import io
import sys
import time


def create_BQ_dataset(client, name, description=""):

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project, name)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.description = "Metadata in support of ETL process"
    
    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # Make an API request.
    
    return dataset

# dataset = create_BQ_dataset(BQ_client, 'etl_metadata','Metadata in support of ETL process')
# print("Created dataset {}.{} with description {}".format(client.project, dataset.dataset_id, 
#                                                         dataset.description))

def delete_BQ_dataset(client, name):

    dataset_id = '{}.{}'.format(client.project, name)

    # Use the delete_contents parameter to delete a dataset and its contents.
    # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
    client.delete_dataset(
        dataset_id, delete_contents=True, not_found_ok=True
    )  

# delete_BQ_dataset(BQ_client, 'etl_metadata')
# print("Deleted dataset '{}'.".format(dataset_id))


def BQ_table_exists(BQ_client, project, dataset, table):
    table_id = "{}.{}.{}".format(project, dataset, table)

    try:
        BQ_client.get_table(table_id)  # Make an API request.
        return True
    except NotFound:
        return False

# if BQ_table_exists(BQ_client, 'etl_metadata', 'etl_metadata'):
#     print("Table already exists.")
# else:
#     print("Table is not found.")


def create_BQ_table(client, project, dataset, table, schema):

    table_id = "{}.{}.{}".format(project, dataset, table)

    table = bigquery.Table(table_id, schema=schema)

    table = client.create_table(table)  # Make an API request.
    return table


def delete_BQ_Table(client, project, dataset, table):
    table_id = "{}.{}.{}".format(project, dataset, table)

    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.

def load_BQ_from_json(client, project, dataset, table, json_rows, aschema):
    table_id = "{}.{}.{}".format(project, dataset, table)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.schema = aschema
    
    # Convert to
    data = io.StringIO(json_rows)
    #     print(json_rows)
    try:
        job = client.load_table_from_file(data, table_id, job_config=job_config)
        while not job.result().state == 'DONE':
            print('Waiting for job done. Status: {}'.format(job.state))
            time.sleep(15)
        result = job.result()
    except:
        print("Error loading table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
              file=sys.stdout, flush=True)
        raise

    return result


# csv_rows is newline delimited csv data
def load_BQ_from_CSV(client, dataset, table, csv_rows, aschema):

    table_id = "{}.{}.{}".format(client.project, dataset, table)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.schema = aschema

    # Convert to
    data = io.StringIO(csv_rows)
    #     print(json_rows)
    try:
        job = client.load_table_from_file(data, table_id, job_config=job_config)
        while not job.result().state == 'DONE':
            print('Waiting for job done. Status: {}'.format(job.state))
            time.sleep(15)
        result = job.result()
    except:
        print("Error loading table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
              file=sys.stdout, flush=True)
        raise
    return job

# load a file at some uri. Assumes a csv or tsv (default).
def load_BQ_from_uri(client, dataset, table, uri, schema, delimiter='\t', skip=1, write_disposition = 'WRITE_APPEND' ):

    table_id = "{}.{}.{}".format(client.project, dataset, table)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = write_disposition
    job_config.field_delimiter = delimiter
    job_config.skip_leading_rows = skip
    job_config.schema = schema

    try:
        job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        while not job.result().state == 'DONE':
            print('Waiting for job done. Status: {}'.format(job.state))
            time.sleep(15)
        result = job.result()
    except:
        print("Error loading table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
              file=sys.stdout, flush=True)


# load a file at some uri. Assumes a csv or tsv (default).
def query_BQ(client, dataset, table, sql, write_disposition = 'WRITE_APPEND' ):

    table_id = "{}.{}.{}".format(client.project, dataset, table)

    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = write_disposition

    try:
        job = client.query(sql, job_config=job_config)
    except:
        print("Error loading table: {},{},{}".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]),
              file=sys.stdout, flush=True)

    result = job.result()
    return result