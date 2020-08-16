#!/bin/bash

# Example command that extracts a BQ table GCS as a tsv
# Use this command to extract IndexD manifest table in BQ to GCS
bq extract -F tab canceridc-data:idc_tcia.IndexD_manifest gs://etl_process/idc_wave0_*.tsv