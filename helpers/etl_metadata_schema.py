from google.cloud import bigquery

etl_metadata_schema = [
    bigquery.SchemaField("SOPInstanceUID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("TCIA_CollectionID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("IDC_CollectionID", "STRING", mode="REQUIRED"),

    bigquery.SchemaField("GCS_URLs", "RECORD", mode="REQUIRED",
        fields=[
            bigquery.SchemaField("StudyInstanceUID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SeriesInstanceUID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SOPInstanceUID", "STRING", mode="REQUIRED"),
        ]
    ),
    bigquery.SchemaField("DICOM_STORE_URLs", "RECORD", mode="REQUIRED",
        fields=[
            bigquery.SchemaField("StudyInstanceUID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SeriesInstanceUID", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SOPInstanceUID", "STRING", mode="REQUIRED"),
        ],
     ),
    bigquery.SchemaField("INDEXD_GUIDSs", "RECORD", mode="REQUIRED",
         fields=[
             bigquery.SchemaField("StudyInstanceUID", "STRING", mode="NULLABLE"),
             bigquery.SchemaField("SeriesInstanceUID", "STRING", mode="NULLABLE"),
             bigquery.SchemaField("SOPInstanceUID", "STRING", mode="NULLABLE"),
         ],
     ),
    bigquery.SchemaField("IDC_Version", "STRING", mode="REPEATED"),
    bigquery.SchemaField("GCS_Generation", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("CRC32C_Hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("MD5_Hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Instance_Size", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Time_Created", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Time_Updated", "TIMESTAMP", mode="REQUIRED")
]
