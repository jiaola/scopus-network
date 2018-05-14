import bonobo
from pymongo import MongoClient
from .elsapi import *
from .transformers import *


def extract_id(args):
    return args['dc:identifier'].split(':')[1]


def extract_author_from_row(args):
    return args[1]


def load(args):
    """Placeholder, change, rename, remove... """
    print(args)


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        get_docs_by_year(2018, True),
        extract_id,
        get_document,
        FilterDuplicate(collection='document', field='_id'),
        MongoWriter(collection='document')
    )
    # Author
    graph.add_chain(
        get_authors_from_doc,
        FilterDuplicate(collection='author', field='@auid'),
        lambda args: args['@auid'],
        get_author,
        MongoWriter(collection='author'),
        # bonobo.JsonWriter('results/authors.json'),
        _input=get_document
    )
    # Author Affiliation
    # graph.add_chain(
    #     get_author_affl,
    #     bonobo.UnpackItems(0),
    #     bonobo.CsvWriter('results/author-affl.csv'),
    #     _input=get_author
    # )
    # # Affiliations - Skip. Instead, use the affiliation API to retrieve JHU affiliations
    # graph.add_chain(
    #     get_author_affl,
    #     FilterDuplicate(collection='affiliation', field='affiliation'),
    #     lambda args: args['affiliation'],
    #     get_affiliation,
    #     MongoWriter(collection='affiliation'),
    #     _input=get_author
    # )
    # Serial By ID
    graph.add_chain(
        lambda args: args['coredata'].get('source-id', None),
        FilterDuplicate(collection='serial'),
        get_serial,
        MongoWriter(collection='serial'),
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
