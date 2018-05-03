import os
from .elsapy import elsclient
from .elsapy.elsprofile import ElsAuthor, ElsAffil
from .elsapy.elsdoc import FullDoc, AbsDoc
from .elsapy.elssearch import ElsSearch
import json

def get_docs_by_year(year, get_all=False):
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    doc_search = ElsSearch('af-id(60005248)', 'scopus', {'date': year})
    print(doc_search.uri)
    doc_search.execute(client, get_all)
    print(doc_search.results)


if __name__ == '__main__':
    get_docs_by_year(2016)