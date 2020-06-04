#!/usr/bin/env python

# Clone a TCIA collection to GCS

from google.cloud import storage
import time
import os,sys,json
sys.path.append(os.environ['CLONE_TCIA'])
from helpers.cloner import copy_collection
import argparse
import subprocess
from collections import OrderedDict
PROJECT = 'idc-dev-etl'

def main(args):
    TCIA_NAME = args.collection
    processes = args.processes
    
    storage_client = storage.Client(project=PROJECT)

    collection_name = TCIA_NAME.replace(' ','_')
    idc_bucket_name = 'idc-tcia-{}'.format(collection_name.lower().replace('_','-'))

    bucket_url = "gs://{}".format(idc_bucket_name)

    bucket = storage_client.bucket(idc_bucket_name)
    if not bucket.exists():
        new_bucket = storage_client.create_bucket(bucket)
        new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        new_bucket.patch()
                
    start = time.time()
    (compressed, uncompressed, series_statistics) = copy_collection(TCIA_NAME, processes, storage_client, PROJECT)
    end = time.time()
    elapsed = end - start

    # Sum the validation results over all series
    validated = OrderedDict()
    for key in series_statistics[0]['validation']:
        validated[key] = 0
    for series in series_statistics:
        for key in series['validation']:
            validated[key] += series['validation'][key]

    print('{}'.format(idc_bucket_name))
    print("Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format(compressed,
        uncompressed, float(compressed)/float(uncompressed) if float(uncompressed) > 0.0 else 1.0, file=sys.stdout), flush=True)
    print("Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format(elapsed, compressed/elapsed), file=sys.stdout, flush=True)
    for key in validated:
        print('{:30} {}'.format(key, validated[key]))

    with open(os.environ['SERIES_STATISTICS'],'w') as f:
        print('{}'.format(idc_bucket_name), file=f)
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

if __name__ == "__main__":
    parser =argparse.ArgumentParser()
    parser.add_argument('--collection','-c', help='Collection name as returned by TCIA /query/getCollectionValues API')
    parser.add_argument('--processes','-p', type=int, default=4, help='Number of worker processes')
    argz = parser.parse_args()
    print("{}".format(argz), file=sys.stdout)
    main(argz)
