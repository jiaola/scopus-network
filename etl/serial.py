import bonobo
from pymongo import MongoClient
from .elsapi import *
from .transformers import *


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
        bonobo.CsvReader('data/2018_ref_pub.csv'),
        FilterSerialTitle(),
        lambda title, count: title,
        get_serial_by_title,
        FilterDuplicate(collection='serial', field='_id', database='scopus'),
        MongoWriter(collection='serial', database='scopus'),
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
