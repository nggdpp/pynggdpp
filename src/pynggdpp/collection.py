#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import sys
import logging
import requests
import os

from pynggdpp import __version__

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "public-domain"

_logger = logging.getLogger(__name__)


def ndc_collection_type_tag(tag_name, include_type=True):
    """
    Queries the ScienceBase Vocabulary for a particular NDC collection type and returns the JSON structure that can
    be used to filter items by that tag.

    :param tag_name: (string) Simple name of the tag
    :param include_type: (true/false) Specify whether or not to include type specification in the query
    :return: JSON structure of the tag used in querying ScienceBase Catalog
    """
    vocab_search_url = f'{os.environ["SB_VOCAB_PATH"]}/' \
                       f'{os.environ["SB_VOCAB_NDC_ID"]}/' \
                       f'terms?nodeType=term&format=json&name={tag_name}'
    r_vocab_search = requests.get(vocab_search_url).json()
    if len(r_vocab_search['list']) == 1:
        tag = {'name':r_vocab_search['list'][0]['name'],'scheme':r_vocab_search['list'][0]['scheme']}
        if include_type:
            tag['type'] = 'theme'
        return tag
    else:
        return None


def ndc_get_collections(parentId=os.environ['SB_CATALOG_NDC_ID'], fields=os.environ['SB_CATALOG_DEFAULT_PROPS'], collection_id=None):
    """
    Query ScienceBase for National Digital Catalog collection metadata records.

    :param parentId: (string) Parent container identifier in ScienceBase; defaults to an environment variable setting
    :param fields: (string) Comma-separated list of properties to include in the return; defaults to a set controlled
    by an environment variable
    :param collection_id: (string) Optional specific collection ID for cases when only one particular collection is
    desired
    :return: (list of dictionaries) Returns the list of items from the ScienceBase API
    """
    nextLink = f'{os.environ["SB_CATALOG_PATH"]}&' \
                           f'fields={fields}&' \
                           f'folderId={parentId}&' \
                           f"filter=tags%3D{ndc_collection_type_tag('ndc_collection',False)}"
    if collection_id is not None:
        nextLink= f"{nextLink}&id={collection_id}"

    collectionItems = list()

    while nextLink is not None:
        r_ndc_collections = requests.get(nextLink).json()

        if "items" in r_ndc_collections.keys():
            collectionItems.extend(r_ndc_collections["items"])

        if "nextlink" in r_ndc_collections.keys():
            nextLink = r_ndc_collections["nextlink"]["url"]
        else:
            nextLink = None

    return collectionItems


def collection_metadata_summary(collection=None, collection_id=None):
    """
    Packages a collection_meta object containing high level summary metadata for collections to be infused into
    individual record properties.

    :param collection: Dictionary object containing the collection details from the ndc_collections collection for
    cases where this information has already been retrieved in a workflow.
    :param collection_id: Collection identifier (ScienceBase ID) for a collection in UUID form used to retrieve the
    collection when needed.
    :return: Dictionary object containing summary metadata following a convention of property names used to infuse
    collection metadata into individual collection record properties
    """
    if collection is None:
        collection_record = ndc_get_collections(collection_id=collection_id, fields="title,contacts,dates")
        if collection_record is None:
            return {"Error": "Collection record could not be retrieved or is not a valid NDC collection"}

        if len(collection_record) > 1:
            return {"Error": "Query for collection returned more than one for some reason"}

        collection_record = collection_record[0]
    else:
        collection_record = collection

    collection_meta = dict()
    collection_meta["ndc_collection_id"] = collection_record["id"]
    collection_meta["ndc_collection_title"] = collection_record["title"]
    collection_meta["ndc_collection_link"] = collection_record["link"]["url"]
    collection_meta["ndc_collection_last_updated"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "lastUpdated"), None)
    collection_meta["ndc_collection_created"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "dateCreated"), None)

    if "contacts" in collection_record.keys():
        collection_meta["ndc_collection_owner"] = [c["name"] for c in collection_record["contacts"]
                               if "type" in c.keys() and c["type"] == "Data Owner"]
        if len(collection_meta["ndc_collection_owner"]) == 0:
            collection_meta["ndc_collection_improvements_needed"] = ["Need data owner contact in collection metadata"]
    else:
        collection_meta["ndc_collection_improvements_needed"] = ["Need contacts in collection metadata"]

    return collection_meta


def parse_args(args):
    """
    Parse command line parameters

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
