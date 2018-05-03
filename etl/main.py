#!/usr/bin/env python

import requests
import os
from urllib import parse
import csv
from lxml import etree
from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil

API_KEY = os.environ['SCOPUS_APIKEY']
JHU_ID = '60005248'
api_url_base = ''
HEADERS = {'Content-Type': 'application/json', 'Accept': 'application/json'}

def main():
    """
    Main ETL script definition
    :return: None
    """
    author_ids = set()

    docs = fetch_docs_by_year(2018)
    doc_file = open('2018-doc.csv', 'w')
    doc_auth_file = open('2018-doc-author.csv', 'w')
    doc_writer = csv.writer(doc_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    doc_writer.writerow(['scopus_id', 'eid', 'title', 'subtype', 'subtype_desc', 'serial_id', 'serial_issn', 'serial_eissn', 'volume', 'issue', 'date', 'doi', 'citedby'])
    doc_auth_writer = csv.writer(doc_auth_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    doc_auth_writer.writerow(['doc_id', 'author_id', 'seq'])
    for doc in docs:
        row = list(map(doc.get, ['dc:identifier', 'eid', 'dc:title', 'subtype', 'subtypeDescription', 'source-id', 'prism:issn', 'prism:eIssn', 'prism:volume', 'prism:issueIdentifier', 'prism:coverDate', 'prism:doi', 'citedby-count']))
        if row[0] is None:
            print(row)
        else:
            row[0] = row[0].split(':')[1]
            doc_writer.writerow(row)
            authors = author_by_doc(row[0])
            for author in authors:
                doc_auth_writer.writerow([row[0], author[0], author[1]])
                author_ids.add(author[0])
    doc_auth_file.close()
    doc_file.close()

    authors = []
    affiliations = []
    affl_ids = set()
    for aid in author_ids:
        author, affiliation = author_by_id(aid)
        authors.append(author)
        if not affiliation[0] in affl_ids:
            affiliations.append(affiliation)
            affl_ids.add(affiliation[0])
    author_file = open('2018-author.csv', 'w')
    author_writer = csv.writer(author_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    author_writer.writerow(['id', 'first_name', 'last_name', 'full_name', 'affiliation_id'])
    for author in authors:
        author_writer.writerow(author)
    author_file.close()

    affl_file = open('2018-affiliations.csv', 'w')
    affl_writer = csv.writer(affl_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    affl_writer.writerow(['id', 'name', 'type', 'parent'])
    for affiliation in affiliations:
        affl_writer.writerow(affiliation)
    affl_file.close()

def citations_by_doc(doc_id):
    api_url = 'https://api.elsevier.com/content/abstract/scopus_id/' + doc_id
    params = {'view': 'REF', 'apiKey': API_KEY}
    res = requests.get(api_url, params=params, header={'Accept': 'application/json'})



def author_by_doc(doc_id):
    api_url = 'https://api.elsevier.com/content/abstract/scopus_id/' + doc_id
    params = {'field': 'author,affiliation', 'apiKey': API_KEY}
    res = requests.get(api_url, params=params)
    if res.status_code != 200:
        print('[!]{0} author_by_doc request failed: {1}'.format(res.status_code, doc_id))
        return
    ns = {'sc': 'http://www.elsevier.com/xml/svapi/abstract/dtd'}
    root = etree.XML(res.content)
    authors = []
    for author in root.xpath(".//sc:authors/sc:author", namespaces=ns):
        # Only extract authors affiliated with Hopkins
        if author.xpath(f"sc:affiliation[@id='{JHU_ID}']", namespaces=ns):
            authors.append([author.get('auid'), author.get('seq')])
    return authors


def author_by_id(author_id):
    client = ElsClient(API_KEY)
    author = ElsAuthor(author_id=author_id)
    if not author.read(client):
        print("[!]Read author failed: {0}", author_id)
    data = [author.int_id, author.first_name, author.last_name, author.full_name]
    affl_data = []
    affl_history = author.data['author-profile']['affiliation-history']['affiliation']
    # Use the first JHU affiliation - This might not be accurate
    affl = None
    if isinstance(affl_history, dict):
        affl = affl_history
    else:
        for affiliation in affl_history:
            if affiliation['@affiliation-id'] == JHU_ID:
                affl = affiliation
            elif '@parent' in affiliation and affiliation['@parent'] == JHU_ID:
                affl = affiliation
                break
    if affl is None:
        print("[!]Can't find JHU affiliation for {0}", author_id)
        data.append(None)
    else:
        data.append(affl['@affiliation-id'])
    #print(affl_history) if affl['@affiliation-id'] == JHU_ID
    parent = affl['@parent'] if '@parent' in affl else None
    affl_data = [affl['@affiliation-id'], affl['ip-doc']['preferred-name']['$'], affl['ip-doc']['@type'], parent]
    print(affl_data)
    return data, affl_data

def fetch_docs_by_year(year):
    api_url = 'https://api.elsevier.com/content/search/scopus'
    params = {'query': f'af-id({JHU_ID})', 'apiKey': API_KEY, 'date': year}
    url = api_url + '?' + parse.urlencode(params)
    docs = []
    while True:
        next_url, entries = fetch_docs(url)
        docs.extend(entries)
        if next_url is None:
            break
        else:
            url = next_url
            break  # TODO: Remove this line
    return docs


def fetch_docs(url):
    res = requests.get(url, headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
    if res.status_code != 200:
        print('[!][{0} author_by_affiliation request failed: {1}'.format(res.status_code, url))
    data = res.json()
    next_link = [x for x in data['search-results']['link'] if x['@ref'] == 'next']
    next_url = next_link[0]['@href'] if len(next_link) > 0 else None
    entries = data['search-results']['entry']
    return next_url, entries


if __name__ == '__main__':
    main()
