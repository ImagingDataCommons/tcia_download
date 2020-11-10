from google.cloud import bigquery
import argparse
import json
import os
import sys


def gen_bundles_manifest(args):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=args.project)

    with open(args.sql_file) as f:
        query = f.read()

    query_job = client.query(query)  # Make an API request.

    series_lengths = {}

    with open(args.csv_file, 'w') as f:
        print("bundle_names, ids", file=f )
        for row in query_job:
            if row['ids'][0].startswith('dg'):
                # It's a series
                series_lengths[row['bundle_names']] = len(row['ids'])
                if len(row['ids']) <= args.max_instances:
                    # Index this series
                    print("{},{}".format(json.dumps(row['bundle_names']).replace('"',''),json.dumps(row['ids']).replace('"','')), file=f)
                else:
                    print("Series {} too long: {}".format(row['bundle_names'], len(row['ids'])))
            else:
                # It's a study
                study_length = sum([series_lengths[series] for series in row['ids']])
                if study_length <= args.max_instances:
                    # Index this study
                    print("{},{}".format(json.dumps(row['bundle_names']).replace('"',''),json.dumps(row['ids']).replace('"','')), file=f)
                else:
                    print("Study {} too long: {}".format(row['bundle_names'], study_length))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sql_file', default='{}/sql/gen_IndexD_bundle_manifest.sql'.format(os.environ['PWD']),
                        help="File containing SQL for this query")
    parser.add_argument('--csv_file', default='{}/tables/idc_mvp_wave0_bundle_manifest.csv'.format(os.environ['PWD']),
                        help="CSV file in which to save results")
    parser.add_argument('--collections', default='all'.format(os.environ['PWD']),
                        help="File containing list of IDC collection IDs or 'all' for all collections")
    parser.add_argument('--max_instances', default=3000, type=int, help="Maximum number of instances in a series or study")
    parser.add_argument('--project', default="idc-dev-etl")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    result = gen_bundles_manifest(args)