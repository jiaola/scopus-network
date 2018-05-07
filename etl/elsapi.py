import os
from .elsapy import elsclient
from .elsapy.elsprofile import ElsAuthor, ElsAffil
from .elsapy.elsdoc import FullDoc, AbsDoc, ElsAbstract, ElsSerial
from .elsapy.elssearch import ElsSearch

from .elsapy import log_util

logger = log_util.get_logger(__name__)


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


def get_document(doc_id):
    """Retrieves a document"""
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    doc = ElsAbstract(scopus_id=doc_id)
    doc.read(client)
    return doc.data


def get_doc_refs(doc_id):
    """Retrieves references of a document"""
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    refs = ElsAbstract(scopus_id=doc_id, params={'view': 'REF'})
    refs.read(client)
    if refs.data is not None:  # Some documents have no reference data
        for ref in refs.data.get('references', {}).get('reference', []):
            yield {'doc': doc_id, 'ref': ref['scopus-id'], 'seq': ref['@id']}


def get_doc_authors(doc_id):
    """Retrieves document-author relationship in tuples"""
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    abstract = ElsAbstract(scopus_id=doc_id, params={'field': 'author,affiliation'})
    abstract.read(client)
    for author in abstract.authors:
        yield {'doc': doc_id, 'author': author.get('@auid', None), 'seq': author.get('@seq', None)}


def get_author(author_id):
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    author = ElsAuthor(author_id=author_id)
    if not author.read(client):
        logger.error("[!]Read author failed: %s", author_id)
    return author.data


def get_author_affl(author_data):
    author_id = author_data['coredata']['dc:identifier']
    author_id = author_id[author_id.find(':') + 1:]
    if 'affiliation-history' in author_data:
        affl_history = author_data.get('affiliation-history', {}).get('affiliation', {})
        if isinstance(affl_history, dict):
            yield {'author': author_id, 'affiliation': affl_history['@id'], 'seq': 1}
        else:
            for idx, val in enumerate(affl_history):
                yield {'author': author_id, 'affiliation': val['@id'], 'seq': idx+1}
    else:
        affl_history = author_data['affiliation-current']
        yield {'author': author_id, 'affiliation': affl_history['@id'], 'seq': 1}


def get_affiliation(affl_id):
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    affl = ElsAffil(affil_id=affl_id)
    if not affl.read(client):
        logger.error("[!]Read affiliation failed: %s", affl_id)
    yield affl.data
    # retrieve ancestors
    parent_id = affl.parent
    while parent_id is not None:
        parent = ElsAffil(affil_id=parent_id)
        if not parent.read(client):
            logger.error("[!]Read affiliation failed: %s", parent_id)
        yield parent.data
        parent_id = parent.parent


def get_serial(serial_id):
    logger.info('getting serial: ' + serial_id)
    client = elsclient.ElsClient(os.environ['SCOPUS_APIKEY'])
    serial = ElsSerial(scopus_id=serial_id, params={'view': 'STANDARD'})
    if not serial.read(client):
        logger.error("[!]Read serial failed: %s", serial_id)
    if 'error' in serial.data:
        logger.error("[!]Read serial with error: %s, %s", serial.data['error'], serial_id)
    else:
        return serial.data['entry']['0']


if __name__ == '__main__':
    # get_docs_by_year('1966', True)
    # print(get_doc_authors('85044277409'))
    # print(get_author('7203039214'))
    print(get_doc_refs('85041118154'))
