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

# This is the schema for the idc_tcia_DICOM_metadata BQ table
from google.cloud import bigquery

auxilliary_metadata_schema = [
    bigquery.SchemaField(
        name="SOPInstanceUID",
        field_type="STRING",
        mode="REQUIRED",
        description="SOPInstanceUID of this instance"),
    bigquery.SchemaField(
        name="TCIA_API_CollectionID",
        field_type="STRING",
        mode="REQUIRED",
        description="The collection ID of this instance's collection as expected by the TCIA API"),
    bigquery.SchemaField(
        name="IDC_Webapp_CollectionID",
        field_type="STRING",
        mode="REQUIRED",
        description="The collection ID of this instance's collection as used internally by the IDC web app"),
    bigquery.SchemaField(
        name="GCS_URL",
        field_type="STRING",
        mode="REQUIRED",
        description="The URL of the GCS object containing this instance"),
    bigquery.SchemaField(
        name="GCS_Bucket",
        field_type="STRING",
        mode="REQUIRED",
        description="The GCS bucket for the collection containing this instance"),
    bigquery.SchemaField(
        name="GCS_Region",
        field_type="STRING",
        mode="REQUIRED",
        description="The GCS region of the bucket containing this instance"),
    bigquery.SchemaField("CRDC_UUIDs", "RECORD", mode="REQUIRED",
                         fields=[
                             bigquery.SchemaField(
                                 name="Study",
                                 field_type="STRING",
                                 mode="NULLABLE",
                                 description="The CRDC UUID of a DRS object for the study containing this instance"),
                             bigquery.SchemaField(
                                 name="Series",
                                 field_type="STRING",
                                 mode="NULLABLE",
                                 description="The CRDC UUID of a DRS object for the series containing this instance"),
                             bigquery.SchemaField(
                                 name="Instance",
                                 field_type="STRING",
                                 mode="NULLABLE",
                                 description="The CRDC UUID of a DRS object for this instance"),
                         ],
                    ),
    bigquery.SchemaField(
        name="IDC_Version",
        field_type="STRING",
        mode="REQUIRED",
        description="The IDC version of this instance"),
    bigquery.SchemaField(
        name="GCS_Generation",
        field_type="STRING",
        mode="REQUIRED",
        description="The GCS generated generation tag of this instance"),
    bigquery.SchemaField(
        name="CRC32C_Hash",
        field_type="STRING",
        mode="REQUIRED",
        description="The crc32c hash of this instance"),
    bigquery.SchemaField(
        name="MD5_Hash", field_type="STRING",
        mode="REQUIRED",
        description="The md5 hash of this instance"),
    bigquery.SchemaField(
        name="Instance_Size",
        field_type="INTEGER",
        mode="REQUIRED",
        description="The size, in bytes, of this instance"),
    bigquery.SchemaField(
        name="Time_Created",
        field_type="TIMESTAMP",
        mode="REQUIRED",
        description="The data/time when the blob containing this instance was created"),
    bigquery.SchemaField(
        name="Time_Updated",
        field_type="TIMESTAMP",
        mode="REQUIRED",
        description="The data/time when the blob containing this instance was last updated")
]
