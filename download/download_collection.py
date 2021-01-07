#!/usr/bin/env python
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

# Download a TCIA collection to GCS
# This is similar to clone_collection.py but intended to be run "locally", that is, on an existing VM.

from google.cloud import storage
import time
import os
import logging

# Don't validate against some reference
os.environ['REF_PREFIX'] = ""

# os.environ['TRG_PREFIX'] = 'idc-tcia-4-'
# os.environ['PROJECT'] = 'idc-dev-etl'
os.environ['TMPDIR'] = 'dicom'

# sys.path.append(os.environ['CLONE_TCIA'])
# from download.cloner import copy_collection
from download.cloner import copy_collection
import argparse
from collections import OrderedDict
from google.api_core.exceptions import Conflict



def main(args):
    tcia_collection_id = args.collection
    processes = args.processes
    dst_prefix = args.dst_prefix
    project = args.project

    logging.debug("In clone_collection, PROJECT: %s", project)

    storage_client = storage.Client(project=project)

    collection_name = tcia_collection_id.replace(' ','_')
    target_bucket_name = '{}{}'.format(dst_prefix, collection_name.lower().replace('_','-'))

    bucket_url = "gs://{}".format(target_bucket_name)

    # Try to create the destination bucket
    new_bucket = storage_client.bucket(target_bucket_name)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = True
    try:
        result = storage_client.create_bucket(new_bucket, location='us')
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        logging.error("Error creating bucket %s: %s",target_bucket_name, e)
        return(-1)

    start = time.time()
    (compressed, uncompressed, series_statistics) = copy_collection(tcia_collection_id, processes, storage_client, project, dst_prefix)
    end = time.time()
    elapsed = end - start

    logging.debug('In clone_collection, got compressed = %s, uncompressed: %s, validation: %s',
                  compressed, uncompressed, series_statistics)

    if len(series_statistics) > 0:
        # Sum the validation results over all series
        validated = OrderedDict()
        for key in series_statistics[0]['validation']:
            validated[key] = 0
        for series in series_statistics:
            for key in series['validation']:
                if not key in validated:
                    logging.warning('Invalid key %s in series %s validation dict: %s',key, series, series['validation'])
                else:
                    validated[key] += series['validation'][key]

        logging.info('%s',target_bucket_name)
        log_string = "Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format( compressed,
            uncompressed, float(compressed)/float(uncompressed) if float(uncompressed) > 0.0 else 1.0)
        logging.info(log_string)
        log_string ="Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format( elapsed, compressed/elapsed)
        logging.info(log_string)
        for key in validated:
            log_string = '{:30} {}'.format(key, validated[key])
            logging.info(log_string)

    else:
        logging.info('%s', target_bucket_name)
        logging.info("Empty collection")


if __name__ == "__main__":
    logging.basicConfig(filename='{}/logs/log.log'.format(os.environ['PWD']), filemode='w', level=logging.INFO)

    parser =argparse.ArgumentParser()
    parser.add_argument('--collection','-c', default='TCGA-READ', help='Collection name as returned by TCIA /query/getCollectionValues API')
    parser.add_argument('--processes','-p', type=int, default=1, help='Number of worker processes')
    parser.add_argument('--project', default='idc-dev-etl', help='Project in which to execute')
    parser.add_argument('--dst_prefix', default='idc-tcia-3-', help='Bucket prefix')
    args = parser.parse_args()
    logging.info("%s", args)

    main(args)
