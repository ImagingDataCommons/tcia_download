#!/bin/bash
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

# This is an example script for uploading a tsv table in GCS to BQ
bq load --skip_leading_rows 1 --source_format CSV --field_delimiter tab \
  idc-dev-etl:idc_tcia_mvp_wave0.idc_tcia_crdc_guids1 \
  gs://crdc_guids/mvp/wave0/dcfprod_idc_manifest_w_guids.tsv \
  guid,md5,size:integer,acl,urls