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

# This is the schema for the idc_tcia_collections_metadata BQ table
from google.cloud import bigquery

data_collections_metadata_schema = [
    bigquery.SchemaField('NBIA_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('TCIA_Webapp_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('TCIA_API_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('IDC_GCS_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('IDC_Webapp_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Status', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Updated', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('Access', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ImageTypes', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Subjects', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('DOI','STRING', mode='NULLABLE'),
    bigquery.SchemaField('CancerType','STRING', mode='NULLABLE'),
    bigquery.SchemaField('SupportingData', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Species', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Location','STRING', mode='NULLABLE'),
    bigquery.SchemaField('Description', 'STRING', mode='NULLABLE')
]