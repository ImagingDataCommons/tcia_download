#!/usr/bin/env
import argparse
import sys
import os
import time
from google.cloud import storage
from google.cloud.exceptions import NotFound

def get_studies(storage_client, bucket_name, prefix='dicom/'):
    studies = []
    iterator = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')
    for page in iterator.pages:
        studies.extend(page.prefixes)
    return studies


def get_series(storage_client, bucket_name):
    studies = get_studies(storage_client, bucket_name)
    series = []

    for study in studies:
        # First get studies; really the prefixes of blobs to the study level.
        iterator = storage_client.list_blobs(bucket_name, prefix=study, delimiter='/')
        for page in iterator.pages:
            series.extend(page.prefixes)
    return series

