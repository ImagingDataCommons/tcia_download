# This is the schema for the idc_tcia_collections_metadata BQ table
from google.cloud import bigquery

analysis_collections_metadata_schema = [
    bigquery.SchemaField('Collection', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('DOI','STRING', mode='NULLABLE'),
    bigquery.SchemaField('CancerType','STRING', mode='NULLABLE'),
    bigquery.SchemaField('Location', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Subjects', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Collections', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('AnalysisArtifactsonTCIA', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Updated', 'DATE', mode='NULLABLE'),
]