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

# This is the schema for the crdc_UUIDs BQ table
from google.cloud import bigquery

crdc_blob_manifests_schema = [
    bigquery.SchemaField(
        name="guid",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="md5",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="size",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="acl",
        field_type="STRING",
        mode="NULLABLE",
        description=""),
    bigquery.SchemaField(
        name="urls",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
]

crdc_series_bundle_manifests_schema = [
    bigquery.SchemaField(
        name="collection",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="SeriesInstanceUID",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="series_guid",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
]

crdc_study_bundle_manifests_schema = [
    bigquery.SchemaField(
        name="collection",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="StudyInstanceUID",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
    bigquery.SchemaField(
        name="study_guid",
        field_type="STRING",
        mode="REQUIRED",
        description=""),
]