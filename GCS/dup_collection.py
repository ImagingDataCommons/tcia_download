#!/usr/bin/env python
from google.cloud import storage
from subprocess import run, PIPE
from google.api_core.exceptions import Conflict
import sys
import argparse

def get_bucket_info(bucket_name, project, storage_client):
    # print('get_series_info args, bucket_name: {}, study: {}, series: {}, storage_client: {}'.format(
    #     bucket_name, study, series, storage_client
    # ))
    blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs()
    bucket_info = {blob.name: blob.crc32c for blob in blobs}
    return bucket_info

def bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, production, client):
    # Try to create the destination bucket
    new_bucket = client.bucket(dst_bucket_name)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = True
    try:
        result = client.create_bucket(new_bucket, requester_pays=production, location='us')
        # If we get here, this is a new bucket
        if production:
            # Add allAuthenticatedUsers
            policy = new_bucket.get_iam_policy(requested_policy_version=3)
            policy.bindings.append({
                "role": "roles/storage.objectViewer",
                "members": {"allAuthenticatedUsers"}
            })
            new_bucket.set_iam_policy(policy)
        return(0)
    except Conflict:
        # Bucket exists
        return(1)
    except:
        # Bucket creation failed somehow
        print("Error creating bucket {}: {}".format(dst_bucket_name, result), flush=True)
        return(-1)


def dup_collection(src_bucket_name, dst_bucket_name, src_project, dst_project, production, storage_client):
    # print("Checking if {} was copied".format(src_bucket_name))
    result = bucket_was_copied(src_bucket_name, dst_bucket_name, src_project, dst_project, production, storage_client)
    if result == 0:
        # Not previously copied
        print("Copying {}".format(src_bucket_name), flush=True)
        try:
            result = run(['gsutil', '-m', '-q', 'cp', '-r',
                    'gs://{}/*'.format(src_bucket_name), 'gs://{}/'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
            print("   {} copied, results: {}".format(src_bucket_name, result), flush=True)
            if result.returncode:
                print('Copy {} failed: {}'.format(result.stderr), flush=True)
                return {"bucket": src_bucket_name, "status": -1}
            return {"bucket": src_bucket_name, "status": 0}
        except:
            print("Error in copying {}: {},{},{}".format(src_bucket_name, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
            return {"bucket": src_bucket_name, "status": -1}

    elif result == 1:
        # Partially copied. Run gsutil cp with the -n no-clobber flag
        print("Continue copying {}".format(src_bucket_name), flush=True)
        try:
            result = run(['gsutil', '-m', '-q', 'cp', '-r', '-n',
                    'gs://{}/*'.format(src_bucket_name), 'gs://{}/'.format(dst_bucket_name)], stdout=PIPE, stderr=PIPE)
            print("   {} copied, results: {}".format(src_bucket_name, result), flush=True)
            if result.returncode:
                print('Copy {} failed: {}'.format(result.stderr), flush=True)
                return {"bucket": src_bucket_name, "status": -1}
            return {"bucket": src_bucket_name, "status": 0}
        except:
            print("Error in copying {}: {},{},{}".format(src_bucket_name, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
            return {"bucket": src_bucket_name, "status": -1}
    else:
        return {"bucket": src_bucket_name, "status": -1}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_bucket_name', default='idc-tcia-1-rider-lung-ct')
    parser.add_argument('--dst_bucket_name', default='idc-tcia-rider-lung-ct')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--production', type=bool, default='True', help="If a production bucket, enable requester pays, allAuthUsers")
    # parser.add_argument('--SA', '-a',
    #         default='{}/.config/gcloud/application_default_config.json'.format(os.environ['HOME']), help='Path to service accoumt key')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.SA

    client = storage.Client(project=args.dst_project)

    dup_collection(args.src_bucket_name, args.dst_bucket_name, args.src_project, args.dst_project, args.production, client)
