import os
from .elsapy import elsclient
from .elsapy.elsprofile import ElsAuthor, ElsAffil
from .elsapy.elsdoc import FullDoc, AbsDoc, ElsAbstract, ElsSerial
from .elsapy.elssearch import ElsSearch
import json


def get_docs_by_year(year, get_all=False):
    all_results = []
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])

    # Split the search since for recent years JHU has publications more than 5,000 each year.
    search_one = ElsSearch('af-id(60005248) AND subjarea(MEDI)', 'scopus', {'date': year})
    search_one.execute(client, get_all)
    all_results += search_one.results

    search_two = ElsSearch('af-id(60005248) AND NOT subjarea(MEDI)', 'scopus', {'date': year})
    search_two.execute(client, get_all)
    all_results += search_two.results

    return all_results


def get_doc_authors(doc_id):
    """Retrieves document-author relationship in tuples"""
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    abstract = ElsAbstract(scopus_id=doc_id, params={'field': 'author,affiliation'})
    abstract.read(client)
    for author in abstract.authors:
        yield (doc_id, author.get('@auid', None), author.get('@seq', None))
    # yield from [(doc_id, author.get('@auid', None), author.get('@seq', None)) for author in abstract.authors]


def get_author(author_id):
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    author = ElsAuthor(author_id=author_id)
    if not author.read(client):
        print("[!]Read author failed: {0}", author_id)
    return author.data
    # data = [author.int_id, author.first_name, author.last_name, author.full_name]
    # affl_data = []
    # affl_history = author.data['author-profile']['affiliation-history']['affiliation']
    # # Use the first JHU affiliation - This might not be accurate
    # affl = None
    # if isinstance(affl_history, dict):
    #     affl = affl_history
    # else:
    #     for affiliation in affl_history:
    #         if affiliation['@affiliation-id'] == JHU_ID:
    #             affl = affiliation
    #         elif '@parent' in affiliation and affiliation['@parent'] == JHU_ID:
    #             affl = affiliation
    #             break
    # if affl is None:
    #     print("[!]Can't find JHU affiliation for {0}", author_id)
    #     data.append(None)
    # else:
    #     data.append(affl['@affiliation-id'])
    # #print(affl_history) if affl['@affiliation-id'] == JHU_ID
    # parent = affl['@parent'] if '@parent' in affl else None
    # affl_data = [affl['@affiliation-id'], affl['ip-doc']['preferred-name']['$'], affl['ip-doc']['@type'], parent]
    # print(affl_data)
    # return data, affl_data


def get_serial(issn):
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    serial = ElsSerial(issn=issn, params={'view': 'STANDARD'})
    if not serial.read(client):
        print("[!]Read serial failed: {0}", issn)
    return serial.data


if __name__ == '__main__':
    # get_docs_by_year('1966', True)
    # print(get_doc_authors('85044277409'))
    # print(get_author('7203039214'))
    print(get_serial('00368075'))
