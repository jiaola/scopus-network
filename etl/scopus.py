import bonobo
from .elsapi import *
from .transformers import *


def transform_identifier(args):
    """Placeholder, change, rename, remove... """
    dc_id = args['dc:identifier']
    return {
        **args,
        'dc:identifier': dc_id[dc_id.find(':') + 1:]
    }


def extract_id(args):
    return args['dc:identifier']


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
        Uniquify(),
        get_document,
        bonobo.JsonWriter('results/docs.json'),
        _input=None,
        _name='write_docs'
    )
    graph.add_chain(
        get_docs_by_year(2018, True),
        #bonobo.Limit(2),
        transform_identifier,
        extract_id,
        _output='write_docs'
    )
    # Refs
    graph.add_chain(
        get_doc_refs,
        bonobo.UnpackItems(0),
        bonobo.CsvWriter('results/doc-refs.csv'),
        _input=extract_id
    )
    graph.add_chain(
        lambda args: args['ref'],
        _input=get_doc_refs,
        _output='write_docs'
    )
    # doc-author
    graph.add_chain(
        get_doc_authors,
        bonobo.UnpackItems(0),
        bonobo.CsvWriter('results/doc-authors.csv'),
        _input=extract_id
    )
    # Author
    graph.add_chain(
        lambda args: args['author'],
        Uniquify(),
        get_author,
        bonobo.JsonWriter('results/authors.json'),
        _input=get_doc_authors
    )
    # Author Affiliation
    graph.add_chain(
        get_author_affl,
        bonobo.UnpackItems(0),
        bonobo.CsvWriter('results/author-affl.csv'),
        _input=get_author
    )
    # Affiliations
    graph.add_chain(
        lambda args: args['affiliation'],
        Uniquify(),
        get_affiliation,
        bonobo.JsonWriter('results/affiliation.json'),
        _input=get_author_affl
    )
    # Serial
    graph.add_chain(
        lambda args: args['coredata'].get('source-id', None),
        Uniquify(),
        get_serial,
        bonobo.JsonWriter('results/serial.json'),
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
        'fs.output': bonobo.open_fs()
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
