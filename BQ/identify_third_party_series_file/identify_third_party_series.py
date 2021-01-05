#!/usr/bin/env
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

import argparse
import sys
import os
import json
from subprocess import run, PIPE

from BQ.collection_ids_file.gen_collection_id_table import build_collections_id_table

# For a specified collection, generate a list of series that came from some third party analysis
def get_internal_series_ids(collection):
    result = run([
        'curl',
        '-d',
        "username=nbia_guest&password=&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe&grant_type=password",
        '-X',
        'POST',
        '-k',
        "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
        ], stdout=PIPE, stderr=PIPE)
    access_token = json.loads(result.stdout.decode())['access_token']
    result = run([
        'curl',
        '-H',
        "Authorization:Bearer {}".format(access_token),
        '-k',
        "https://public.cancerimagingarchive.net/nbia-api/services/getSimpleSearchWithModalityAndBodyPartPaged",
        '-d',
        "criteriaType0=ThirdPartyAnalysis&value0=yes&criteriaType1=CollectionCriteria&value1={}&sortField=subject&sortDirection=descending&start=0&size=100000".format(collection)
        ], stdout=PIPE, stderr=PIPE)
    internal_ids = json.loads(result.stdout.decode())
    return internal_ids

def drill_down(series_ids):
    try:
        result = run([
            'curl',
            '-d',
            "username=nbia_guest&password=&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe&grant_type=password",
            '-X',
            'POST',
            '-k',
            "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
            ], stdout=PIPE, stderr=PIPE)
    except:
        pass
    access_token = json.loads(result.stdout.decode())['access_token']
    try:
        result = run([
            'curl',
            '-H',
            "Authorization:Bearer {}".format(access_token),
            '-k',
            "https://public.cancerimagingarchive.net/nbia-api/services/getStudyDrillDown",
            '-d',
            "&&".join(['list={}'.format(id) for id in series_ids])
            ], stdout=PIPE, stderr=PIPE)
    except:
        pass
    return json.loads(result.stdout.decode())


def get_3rd_party_series_ids(nbia_collection_id):
    dois = []
    count = 0
    third_party_series = []
    internal_ids = get_internal_series_ids(nbia_collection_id)
    for subject in internal_ids["resultSet"]:
        seriesIDs = []
        for study in subject["studyIdentifiers"]:
            seriesIDs.extend(study["seriesIdentifiers"])
        study_metadata = drill_down(seriesIDs)
        for study in study_metadata:
            for series in study["seriesList"]:
                uri = series["descriptionURI"]
                # If it's a doi.org uri, keep just the DOI
                if 'doi.org' in uri:
                    uri = uri.split('doi.org/')[1]
                seriesUID = series["seriesUID"]
                third_party_series.append({"SeriesInstanceUID": seriesUID, "SourceDOI": uri})
                if not uri in dois:
                    dois.append(uri)
                count += 1
    return (third_party_series, dois, count)


def id_3rd_party_series(args):
    if args.dones_file != "":
        try:
            with open(args.dones_file) as f:
                all_third_party_series_ids = json.load(f)
        except:
            os.mknod(args.dones_file)
            all_third_party_series_ids = {}
    else:
        all_third_party_series_ids = {}
    dones = all_third_party_series_ids.keys()
    dois = []
    count = 0

    # Get a table that maps from NBIA collection names to IDC collection names.
    collection_id_map = build_collections_id_table(args)
    for collection in collection_id_map:
        print(collection)
        if not collection["IDC_Webapp_CollectionID"] in dones:
            (third_party_series, collection_dois, collection_count) = get_3rd_party_series_ids(collection["NBIA_CollectionID"])
            all_third_party_series_ids[collection["IDC_Webapp_CollectionID"]] = third_party_series
            dois.extend(collection_dois)
            count += collection_count
            if args.dones_file != "":
                with open(args.dones_file,'w') as f:
                    json.dump(all_third_party_series_ids,f)

    return (all_third_party_series_ids, dois, count)

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--dones_file', default='{}/lists/third_party_series.json'.format(os.environ['PWD']),
    #                     help="File in which to accumulate results")
    parser.add_argument('--dones_file', default='',
                        help="File in which to accumulate results")
    parser.add_argument('--collections', default='{}/../../lists/idc_mvp_wave_0.txt'.format(os.environ['PWD']),
                        help="File containing list of IDC collection IDs or 'all' for all collections")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    results, dois, count = id_3rd_party_series(args)
    print("All DOIs: {}".format(dois))
    print("Total series: {}".format(count))