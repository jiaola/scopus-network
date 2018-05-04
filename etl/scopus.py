import bonobo
from .elsapi import *
from .transformers import *


def extract():
    """Placeholder, change, rename, remove... """
    yield from get_docs_by_year(1966, False)


def transform_identifier(args):
    """Placeholder, change, rename, remove... """
    dc_id = args['dc:identifier']
    return {
        **args,
        'dc:identifier': dc_id[dc_id.find(':') + 1:]
    }


def extract_id(args):
    return args['dc:identifier']


def extract_doc_issn(args):
    if 'prism:eIssn' in args:
        return args['prism:eIssn']
    elif 'prism:issn' in args:
        return args['prism:issn']
    else:
        return None


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
        get_docs_by_year(1966, False),
        bonobo.Limit(5),
        transform_identifier,
        bonobo.JsonWriter('docs.json')
    )
    graph.add_chain(
        extract_id,
        get_doc_authors,
        bonobo.CsvWriter('doc-authors.csv'),
        _input=transform_identifier
    )
    # Author
    graph.add_chain(
        Uniquify(1),
        lambda *args: args[1],
        get_author,
        bonobo.JsonWriter('authors.json'),
        _input=get_doc_authors
    )
    # Author Affiliation
    graph.add_chain(
        get_author_affl,
        bonobo.PrettyPrinter(),
        bonobo.CsvWriter('author-affl.csv'),
        _input=get_author
    )
    # Affiliations
    graph.add_chain(
        Uniquify(1),
        lambda *args: args[1],
        get_affiliation,
        bonobo.JsonWriter('affiliation.json'),
        _input=get_author_affl
    )
    # Serial
    graph.add_chain(
        extract_doc_issn,
        Uniquify(),
        get_serial,
        bonobo.JsonWriter('serial.json'),
        _input=transform_identifier
    )
    # graph.add_chain(
    #     _input=get_author
    # )

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {}


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
