#!/bin/bash
# This is an example script for uploading a tsv table in GCS to BQ
bq load --skip_leading_rows 1 --source_format CSV --field_delimiter tab \
  idc-dev-etl:idc_tcia_mvp_wave0.idc_tcia_crdc_guids \
  gs://crdc_guids/mvp/wave0/dcfprod_idc_manifest_w_guids.tsv \
  guid,md5,size:integer,acl,urls