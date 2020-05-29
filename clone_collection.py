#!/usr/bin/env python

# Clone a TCIA collection to GCS

from google.cloud import bigquery,storage
import time
import os,sys,json
sys.path.append(os.environ['CLONE_TCIA'])
from cloner import copy_collection
import argparse
import subprocess
PROJECT = 'idc-dev-etl'

VALIDATED=1
NO_VALIDATION=0
UNEQUAL_INSTANCE_COUNT=-1
CRC32C_MISMATCH=-2
INVALID_ZIP=-3
SERIES_CONTENT_DIFFERS=-4
SERIES_DOWNLOAD_FAILED=-5
GCS_UPLOAD_FAILED=-6
PREVIOUSLY_DOWNLOADED=-7
ZIP_EXTRACT_FAILED=-8
def main(args):
    TCIA_NAME = args.collection
    processes = args.processes
    
    storage_client = storage.Client(project=PROJECT)

    collection_name = TCIA_NAME.replace(' ','_')
    idc_bucket_name = 'idc-tcia-{}'.format(collection_name.lower().replace('_','-'))

    bucket_url = "gs://{}".format(idc_bucket_name)

    bucket = storage_client.bucket(idc_bucket_name)
    if bucket.exists():
        pass
#        subprocess.run(['gsutil', '-m', '-q', 'rm', '-r', 'gs://{}'.format(idc_bucket_name)])
#        bucket.delete_blobs(bucket.list_blobs())
    else:
        new_bucket = storage_client.create_bucket(bucket)
        new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        new_bucket.patch()
                
    
    start = time.time()
    (compressed, uncompressed, series_statistics) = copy_collection(TCIA_NAME, processes, storage_client, PROJECT)
    end = time.time()
    elapsed = end - start

    print("Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format(compressed, 
        uncompressed, float(compressed)/float(uncompressed) if float(uncompressed) > 0.0 else 1.0, file=sys.stdout))
    print("Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format(elapsed, compressed/elapsed), file=sys.stdout)  
    validated = no_validation = unequal_counts = crc32_mismatch = invalid_zip = series_content_differs = 0
    series_download_failed = gcs_upload_failed = previously_downloaded = zip_extract_failed = 0
    for val in series_statistics:
        if val['validation'] == VALIDATED:
            validated +=1
        elif val['validation'] == NO_VALIDATION:
            no_validation += 1
        elif val['validation'] == UNEQUAL_INSTANCE_COUNT:
            unequal_counts += 1
        elif val['validation'] == CRC32C_MISMATCH:
            crc32_mismatch +=1
        elif val['validation'] == INVALID_ZIP:
            invalid_zip +=1
        elif val['validation'] == SERIES_CONTENT_DIFFERS:
            series_content_differs +=1
        elif val['validation'] == SERIES_DOWNLOAD_FAILED:
            series_download_failed +=1
        elif val['validation'] == GCS_UPLOAD_FAILED:
            gcs_upload_failed +=1
        elif val['validation'] == PREVIOUSLY_DOWNLOADED:
            previously_downloaded +=1
        elif val['validation'] == ZIP_EXTRACT_FAILED:
            zip_extract_failed +=1
        else:
            print("Unknown validation result {} for {}/{}".format(val['validation'], val['study'], val['series']))
    print('Validated: {}, No validation: {}, Unequal counts: {}, CRC32C mismatch: {}, invalid zip: {}, series content differs: {}, series_download_failed: {}, gcs_upload_failed: {}, previously_downloaded: {}, zip_extract_failed: {} '.format(
            validated, no_validation, unequal_counts, crc32_mismatch, invalid_zip, series_content_differs, series_download_failed, gcs_upload_failed, previously_downloaded, zip_extract_failed), file=sys.stdout)

    with open(os.environ['SERIES_STATISTICS'],'w') as f:
        print("Compressed bytes: {:,}, Uncompressed bytes: {:,}, Compression: {:.3}".format(compressed, 
            uncompressed, float(compressed)/float(uncompressed) if float(uncompressed) > 0.0 else 1.0), file=f)
        print("Elapsed time (s):{:.3}, Bandwidth (B/s): {:.3}".format(elapsed, compressed/elapsed), file=f)  
        print('Validated: {}, No validation: {}, Unequal counts: {}, CRC32C mismatch: {}, invalid zip: {}, series content differs: {}, series_download_failed: {}, gcs_upload_failed: {}, previously_downloaded: {}, zip_extract_failed: {} '.format(
            validated, no_validation, unequal_counts, crc32_mismatch, invalid_zip, series_content_differs, series_download_failed, gcs_upload_failed, previously_downloaded, zip_extract_failed), file=f)
        json.dump(series_statistics,f)

if __name__ == "__main__":
    parser =argparse.ArgumentParser()
    parser.add_argument('--collection','-c', help='Collection name as returned by TCIA /query/getCollectionValues API')
    parser.add_argument('--processes','-p', type=int, default=4, help='Number ofworker processes')
    argz = parser.parse_args()
    print(argz)
    main(argz)
