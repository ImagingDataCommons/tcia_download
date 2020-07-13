#!/usr/bin/env

import argparse
import sys
import os
import json
from google.cloud import storage

def main(args):
    client = storage.Client(project=args.project)
    src_bucket = client.bucket(args.src_bucket)
    dst_bucket = client.bucket(args.dst_bucket)
    for blob in list(client.list_blobs(args.src_bucket, prefix=args.src_blob_prefix)):
        if blob.name.rsplit('.')[1] == 'ndjson':
            ndjsons = blob.download_as_string().decode('utf-8').split('\n')
            tsvs = []
            keys = list(json.loads(ndjsons[0]).keys())
            tsvs.append('\t'.join(keys))
            try:
                for ndjson in ndjsons:
                    if ndjson != "":
                        vals = []
                        for val in list(json.loads(ndjson).values()):
                            if type(val) == list :
                                vals.append(','.join(val))
                            else:
                                vals.append(val)
                        try:
                            tsvs.append('\t'.join(vals))
                        except:
                            pass
            except:
                pass
            tsv = '\n'.join(tsvs)

            dst_blob = dst_bucket.blob(blob.name.replace('ndjson','tsv'))
            result = dst_blob.upload_from_string(tsv)




    pass

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--src_bucket', default='etl_process', help='Bucket hold ndjson blobs')
    parser.add_argument('--src_blob_prefix', default='indexD_input_mvp_wave0')
    parser.add_argument('--dst_bucket', default='etl_process', help='Bucket hold tsv blobs')
    parser.add_argument('--project', default='canceridc-data')
    # parser.add_argument('--SA', '-a',
    #         default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    parser.add_argument('--SA', default='', help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    if not args.SA == '':
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA
    main(args)