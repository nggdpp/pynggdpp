#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import sys
import logging
import requests
from geojson import Feature, Point, FeatureCollection
from bs4 import BeautifulSoup
import xmltodict
from datetime import datetime
from gis_metadata.iso_metadata_parser import IsoParser


from pynggdpp import __version__

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "public-domain"

_logger = logging.getLogger(__name__)


def build_point_geometry(coordinates):
    pointGeometry = Point((float(coordinates.split(',')[0]), float(coordinates.split(',')[1])))
    return pointGeometry


def build_ndc_feature(geom, props):
    ndcFeature = Feature(geometry=geom, properties=props)
    return ndcFeature


def list_waf(url, ext='xml'):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]


def ndc_xml_to_geojson(file_url):
    try:
        xmlData = requests.get(file_url).text
        dictData = xmltodict.parse(xmlData, dict_constructor=dict)

        if 'samples' in dictData.keys():
            sample_list = dictData['samples']['sample']
        elif 'historictopomaps' in dictData.keys():
            sample_list = dictData['historictopomaps']['topomap']
        else:
            sample_list = None
            raise Exception("XML file does not contain 'samples'")

        if sample_list is not None:
            feature_list = []
            for sample in sample_list:
                sample['source_file'] = file_url
                sample['build_from_source_date'] = datetime.utcnow().isoformat()
                pointGeometry = build_point_geometry(sample['coordinates'])
                feature_list.append(build_ndc_feature(pointGeometry, sample))

            return FeatureCollection(feature_list)

    except Exception as e:
        return e


def ndc_collection_from_waf(waf_url):
    feature_list = []

    for link in list_waf(waf_url):
        iso_xml = requests.get(link).text
        parsed_iso = IsoParser(iso_xml)

        coordinates = parsed_iso.bounding_box['east'] + ',' + parsed_iso.bounding_box['south']
        pointGeometry = build_point_geometry(coordinates)

        item = {}
        item['title'] = parsed_iso.title
        item['abstract'] = parsed_iso.abstract
        item['place_keywords'] = parsed_iso.place_keywords
        item['thematic_keywords'] = parsed_iso.thematic_keywords
        item['temporal_keywords'] = parsed_iso.temporal_keywords

        feature_list.append(build_ndc_feature(pointGeometry, item))

    waf_feature_collection = FeatureCollection(feature_list)
    return waf_feature_collection


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
