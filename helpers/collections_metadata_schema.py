# This is the schema for the idc_tcia_collections_metadata BQ table
from google.cloud import bigquery

collections_metadata_schema = [
    bigquery.SchemaField('Collection', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('TCIA_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('IDC_CollectionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Webapp_CollectionID', 'STRING', mode='NULLABLE'),
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