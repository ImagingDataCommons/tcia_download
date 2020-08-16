import difflib

import requests
from bs4 import BeautifulSoup
import backoff

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=60)
def get_url(url):  # , headers):
    return requests.get(url)  # , headers=headers)


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=60)
def get_url(url):  # , headers):
    return requests.get(url)  # , headers=headers)

def scrape_tcia_analysis_collections_page():
    URL = 'https://www.cancerimagingarchive.net/tcia-analysis-results/'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find(id="tablepress-10")

    # print(table.prettify())

    rows = table.find_all("tr")

    table = {}
    header = "Collection,DOI,CancerType,Location,Subjects,Collections,AnalysisArtifactsonTCIA,Updated".split(",")

    for row in rows:
        trow = {}
        cols = row.find_all("td")
        for cid, col in enumerate(cols):
            if cid == 0:
                trow[header[0]] = col.find("a").text
                trow[header[1]] = col.find("a")["href"]
            else:
                trow[header[cid + 1]] = col.text
        if len(trow):
            collection = trow.pop('Collection')
            table[collection] = trow
            # table = table + [trow]

    # print(tabulate(table, headers=header))

    print(len(rows))

    # with open("output/analysis_collections.json", "w") as f:
    #     f.write(json.dumps(table, indent=2))

    return table

def scrape_tcia_data_collections_page():
    URL = 'http://www.cancerimagingarchive.net/collections/'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find(id="tablepress-9")

    # print(table.prettify())

    rows = table.find_all("tr")

    table = {}
    header = "Collection,DOI,CancerType,Location,Species,Subjects,ImageTypes,SupportingData,Access,Status,Updated".split(
        ",")

    for row in rows:
        trow = {}
        cols = row.find_all("td")
        for cid, col in enumerate(cols):
            if cid == 0:
                trow[header[0]] = col.find("a").text
                trow[header[1]] = col.find("a")["href"]
                if not trow[header[1]].startswith("http"):
                    trow[header[1]] = "http:" + col.find("a")["href"]
            else:
                trow[header[cid + 1]] = col.text
        if len(trow):
            collection = trow.pop('Collection')
            table[collection] = trow

    # print(tabulate(table, headers=header))

    # print(len(rows))
    #
    # with open("output/collections.json", "w") as f:
    #   f.write(json.dumps(table, indent=2))
    return table

def build_TCIA_to_Description_ID_Table(collections, descriptions):
    '''
    Build a table that maps collections ids from scraped TCIA collection data to collection ids in NBIA collection
    descriptions. The mapping is empirical.
    collections is a dictionary of collection data indexed by collection name
    descriptions is a dictionary of collection names indexed by collection name
    '''

    table = {}
    # Create a table of normalized to original description ids
    description_ids = {id.lower().replace(' ', '-').replace('_', '-'):id for id, data in descriptions.items()}

    for id,data in collections.items():
        if data['Access'] == 'Public' and data['ImageTypes'] != 'Pathology':
            table[id]  = description_ids[difflib.get_close_matches(id.split('(')[0].lower().replace(' ','-').replace('_','-'),
                                                                   list(description_ids.keys()), 1, 0.5)[0]]

    # Yuch!!. Do some fix ups
    table["AAPM RT-MAC Grand Challenge 2019"] = "AAPM-RT-MAC"
    if "CT-ORG" in table:
        table.pop("CT-ORG")
    table["APOLLO-1-VA"] = "APOLLO"

    return table