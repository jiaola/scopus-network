import bonobo
from pymongo import MongoClient
from .elsapi import *
from .transformers import *


def extract_id(args):
    return args['dc:identifier'].split(':')[1]


def load(args):
    print(args)


def extract_authors(args):
    for author in args['results']:
        yield author


def create_author_document(args):
    data = {
        'first_name': args['first_name'],
        'last_name': args['last_name'],
        'scopus_ids': [extract_id(x) for x in args['results']]
    }
    yield data


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        bonobo.CsvReader('data/biophysics/faculty.csv'),
        get_author_by_name,
        create_author_document,
        MongoWriter(collection='jhu-authors', database='assessments'),
    )

    graph.add_chain(
        extract_authors,
        extract_id,
        FilterDuplicate(collection='scopus-authors', database='assessments'),
        get_author,
        MongoWriter(collection='scopus-authors', database='assessments'),
        _input=get_author_by_name
    )

    graph.add_chain(
        extract_id,
        get_docs_by_author,
        extract_id,
        FilterDuplicate(collection='scopus-documents', field='_id', database='assessments'),
        get_document,
        MongoWriter(collection='scopus-documents', database='assessments'),
        _input=extract_authors
    )

    graph.add_chain(
        lambda args: args['coredata'].get('source-id', None),
        FilterDuplicate(collection='scopus-serials', database='assessments'),
        get_serial,
        MongoWriter(collection='scopus-serials', database='assessments'),
        _input=get_document
    )

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {
        'mongodb.client': MongoClient('localhost', 27017)
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
