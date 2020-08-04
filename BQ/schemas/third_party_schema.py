# This is the schema for the idc_tcia_DICOM_metadata BQ table
from google.cloud import bigquery

third_party_schema = [
    bigquery.SchemaField("SeriesInstanceUID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("SourceDOI", "STRING", mode="REQUIRED")
]