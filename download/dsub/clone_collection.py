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

# Clone a TCIA collection to GCS
# This module is intended to be run under dsub, driven by run_tasks.py, and calls cloner.py to do the real work

from google.cloud import storage
import time
import os
import sys
import json

sys.path.append(os.environ['CLONE_TCIA'])
# from download.cloner import copy_collection
from helpers.cloner import copy_collection
import argparse
from collections import OrderedDict
from google.api_core.exceptions import Conflict



def main(args):
    TCIA_NAME = args.collection
    processes = args.processes
    TRG_PREFIX = os.environ['TRG_PREFIX']
    PROJECT = os.environ['PROJECT']

    # print("In clone_collection, PROJECT: {}".format(PROJECT))

    storage_client = storage.Client(project=PROJECT)

    collection_name = TCIA_NAME.replace(' ','_')
    target_bucket_name = '{}{}'.format(TRG_PREFIX, collection_name.lower().replace('_','-'))

    bucket_url = "gs://{}".format(target_bucket_name)

    # bucket = storage_client.bucket(target_bucket_name)
    # if not bucket.exists():
    #     new_bucket = storage_client.create_bucket(bucket)
    #     new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    #     new_bucket.patch()

    # Try to create the destination bucket
    new_bucket = storage_client.bucket(target_bucket_name)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = True
    try:
        result = storage_client.create_bucket(new_bucket, location='us')
        return(0)
    except Conflict:
        # Bucket exists
        pass
    except:
        # Bucket creation failed somehow
        print("Error creating bucket {}: {}".format(target_bucket_name, result), flush=True)
        return(-1)

    start = time.time()
    (compressed, uncompressed, series_statistics) = copy_collection(TCIA_NAME, processes, storage_client, PROJECT)
    end = time.time()
    elapsed = end - start

    # logging.debug('In clone_collection, got compressed = %s, uncompressed: %s, validation: ',
    #               compressed, uncompressed, series_statistics)
    # print('In clone_collection, got compressed: {}, uncompressed: {}, series statistics: {}'.format(
    #               compressed, uncompressed, series_statistics))

    if len(series_statistics) > 0:
        # Sum the validation results over all series
        validated = OrderedDict()
        for key in series_statistics[0]['validation']:
            validated[key] = 0
        for series in series_statistics:
            for key in series['validation']:
                if not key in validated:
                    print('Invalid key {} in series {} validation dict: {}'.format(key, series, series['validation']))
                else:
                    validated[key] += series['validation'][key]

        print('{}'.format(target_bucket_name))
        print("Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format(compressed,
            uncompressed, float(compressed)/float(uncompressed) if float(uncompressed) > 0.0 else 1.0, file=sys.stdout), flush=True)
        print("Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format(elapsed, compressed/elapsed), file=sys.stdout, flush=True)
        for key in validated:
            print('{:30} {}'.format(key, validated[key]))

        with open(os.environ['SERIES_STATISTICS'],'w') as f:
            print('{}'.format(target_bucket_name), file=f)
            print("Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format(compressed,
                uncompressed, float(compressed)/float(uncompressed) if float(uncompressed) > 0.0 else 1.0), file=f)
            print("Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format(elapsed, compressed/elapsed), file=f)
            for key in validated:
                print('{:30} {}'.format(key, validated[key]), file=f)
            print('', file=f)
            for series in series_statistics:
                validation = [series['validation'][key] for key in series['validation']]
                print("study: {}, series: {}, compressed: {}, uncompressed: {}, validation: {}".format(
                    series['study'], series['series'], series['compressed'], series['uncompressed'], json.dumps(validation)), file=f)
    else:
        print('{}'.format(target_bucket_name))
        print("Empty collection")

        with open(os.environ['SERIES_STATISTICS'],'w') as f:
            print('{}'.format(target_bucket_name), file=f)
            print("Empty collection", file=f)



if __name__ == "__main__":
#    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
#    print('GOOGLE_APPLICATION_CREDENTIALS: {}'.format(os.environ['GOOGLE_APPLICATION_CREDENTIALS']), file=sys.stderr, flush=True)
    with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) as f:
        for line in f:
            print(line, file=sys.stderr, flush=True)

    parser =argparse.ArgumentParser()
    parser.add_argument('--collection','-c', help='Collection name as returned by TCIA /query/getCollectionValues API')
    parser.add_argument('--processes','-p', type=int, default=4, help='Number of worker processes')
    argz = parser.parse_args()
    print("{}".format(argz), file=sys.stdout)
    main(argz)
