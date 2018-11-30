#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import sys
import logging
import requests

from pynggdpp import __version__

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "public-domain"

_logger = logging.getLogger(__name__)

sb_vocab_path = 'https://www.sciencebase.gov/vocab'
sb_vocab_id_ndc = '5bf3f7bce4b00ce5fb627d57'

sb_catalog_path = 'https://www.sciencebase.gov/catalog/items?format=json&max=1000'
sb_ndc_id = '4f4e4760e4b07f02db47dfb4'

default_fields_collections = 'title,body,contacts,spatial,files,webLinks,facets'


def ndc_collection_type_tag(tag_name,include_type=True):
    vocab_search_url = f'{sb_vocab_path}/{sb_vocab_id_ndc}/terms?nodeType=term&format=json&name={tag_name}'
    r_vocab_search = requests.get(vocab_search_url).json()
    if len(r_vocab_search['list']) == 1:
        tag = {'name':r_vocab_search['list'][0]['name'],'scheme':r_vocab_search['list'][0]['scheme']}
        if include_type:
            tag['type'] = 'theme'
        return tag
    else:
        return None


def ndc_get_collections(parentId=sb_ndc_id, fields=default_fields_collections, collection_id=None):
    sb_query_collections = f'{sb_catalog_path}&' \
                           f'fields={fields}&' \
                           f'folderId={parentId}&' \
                           f"filter=tags%3D{ndc_collection_type_tag('ndc_collection',False)}"
    if collection_id is not None:
        sb_query_collections = f"{sb_query_collections}&id={collection_id}"
    r_ndc_collections = requests.get(sb_query_collections).json()
    return r_ndc_collections['items']


def collection_metadata_summary(collection=None, *collection_id):
    if collection is None:
        collection_record = ndc_get_collections(collection_id=collection_id)
        if collection_record is None:
            return {"Error": "Collection record could not be retrieved or is not a valid NDC collection"}

        if len(collection_record) > 1:
            return {"Error": "Query for collection returned more than one for some reason"}

        collection_record = collection_record[0]
    else:
        collection_record = collection

    metadata_summary = dict()
    metadata_summary["Collection Title"] = collection_record["title"]
    metadata_summary["Collection Link"] = collection_record["link"]["url"]

    if "contacts" in collection_record.keys():
        metadata_summary["Collection Owner"] = [c["name"] for c in collection_record["contacts"]
                               if "type" in c.keys() and c["type"] == "Data Owner"]
        if len(metadata_summary["Collection Owner"]) == 0:
            metadata_summary["Collection Improvement Needed"] = "Need data owner contact in collection metadata"
    else:
        metadata_summary["Collection Improvement Needed"] = "Need contacts in collection metadata"

    return metadata_summary


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Just a Fibonnaci demonstration")
    parser.add_argument(
        '--version',
        action='version',
        version='pynggdpp {ver}'.format(ver=__version__))
    parser.add_argument(
        dest="n",
        help="n-th Fibonacci number",
        type=int,
        metavar="INT")
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.debug("Starting crazy calculations...")
    print("The {}-th Fibonacci number is {}".format(args.n, fib(args.n)))
    _logger.info("Script ends here")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
