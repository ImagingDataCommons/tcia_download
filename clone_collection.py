from google.cloud import bigquery,storage
import time
import os,sys,json
sys.path.append(os.environ['CLONE_TCIA'])
from cloner import copy_collection
import argparse

PROJECT = 'idc-dev-etl'

VALIDATED=1
NO_VALIDATION=0
UNEQUAL_INSTANCE_COUNT=-1
CRC32C_MISMATCH=-2

def main(args):
    TCIA_NAME = args.collection
    processes = args.processes
    
    storage_client = storage.Client(project=PROJECT)

    collection_name = TCIA_NAME.replace(' ','_')
    idc_bucket_name = 'idc-tcia-{}'.format(collection_name.lower().replace('_','-'))

    bucket_url = "gs://{}".format(idc_bucket_name)

    bucket = storage_client.bucket(idc_bucket_name)
    if bucket.exists():
        bucket.delete_blobs(bucket.list_blobs())
    else:
        bucket.create()
    
    start = time.time()
    (compressed, uncompressed, series_statistics) = copy_collection(TCIA_NAME, processes, storage_client, PROJECT)
    end = time.time()
    elapsed = end - start
    print("Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format(compressed, 
            uncompressed, compressed/uncompressed))
    print("Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format(elapsed, compressed/elapsed))  
    validated = no_validation = unequal_counts = crc32_mismatch = 0
    for val in series_statistics:
        if val['validation'] == VALIDATED:
            validated +=1
        elif val['validation'] == NO_VALIDATION:
            no_validation += 1
        elif val['validation'] == UNEQUAL_INSTANCE_COUNT:
            unequal_counts += 1
        elif val['validation'] == CRC32C_MISMATCH:
            crc32_mismatch +=1
        else:
            print("Unknown validation result for {}/{}".format(val['study'],val['series']))
    print('Validated: {}, No validation: {}, Unequal counts: {}, CRC32C mismatch: {}'.format(
            validated, no_validation, unequal_counts, crc32_mismatch))

    with open(os.environ['SERIES_STATISTICS'],'w') as f:
        json.dump(series_statistics,f)

if __name__ == "__main__":
    parser =argparse.ArgumentParser()
    parser.add_argument('--collection','-c')
    parser.add_argument('--processes','-p', type=int, default=8)
    argz = parser.parse_args()
    print(argz)
    main(argz)
