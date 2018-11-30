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
import csv
import json
import os
from pymongo import MongoClient
import pandas as pd


from pynggdpp import __version__

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "public-domain"

_logger = logging.getLogger(__name__)


def build_point_geometry(coordinates):
    try:
        pointGeometry = Point((float(coordinates.split(',')[0]), float(coordinates.split(',')[1])))
    except:
        pointGeometry = Point(None)
    return pointGeometry


def build_ndc_feature(geom, props):
    ndcFeature = Feature(geometry=geom, properties=props)
    return ndcFeature


def list_waf(url, response=None, ext='xml'):
    if response is None:
        page = requests.get(url).text
    else:
        page = response.text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]


def build_ndc_feature_collection(feature_list):
    return FeatureCollection(feature_list)


def file_meta(collection_id, files):
    """
    Retrieve and package metadata for a set of file (expects list of file objects in ScienceBase Item format).

    Args:
        collection_id: (str) ScienceBase ID of the collection.
        files: (list of dicts) file objects in a list from the ScienceBase collection item

    Note: In the workflow, this function is used to pre-build ndc_files, a database collection where file information
    is cached for later processing. It processes through the file objects in the list and adds information
    based on retrieving and examining the file via its url.
    """

    # Put a single file object into a list
    if isinstance(files, dict):
        files = [files]

    # Check for explicit Source Data flagging in the collection
    explicit_source_files = [f["pathOnDisk"] for f in files if "title" in f.keys() and f["title"] == "Source Data"]

    # Set up evaluated file list
    evaluated_file_list = list()

    for file in files:
        # Copy in all source properties
        metadata = file

        # Add context
        metadata["collection_id"] = collection_id
        metadata["Date Checked"] = datetime.utcnow().isoformat()

        # Retrieve the file to check things out
        r_file = requests.get(file["url"])

        # Add important bits from the response
        metadata["Content-Type"] = r_file.headers["Content-Type"]
        metadata["Content-Disposition"] = r_file.headers["Content-Disposition"]
        metadata["encoding"] = r_file.encoding

        # Put in latin1 encoding as a backup, this lets us read the file but may throw in a few content issues
        if metadata["encoding"] == None:
            metadata["encoding"] = "latin1"

        # Set up the response text for processing
        r_text = r_file.text
        r_lines = r_text.splitlines()
        line_reader = csv.reader(r_lines)

        # Pull out the first line of text file responses and analyze for content
        if metadata["Content-Type"].split("/")[0] == "text":
            try:
                for index,line in enumerate(line_reader):
                    if index > 0:
                        break
                    metadata["first_line"] = line[0]

                # Check for quote characters
                if metadata["first_line"].find('"') != -1:
                    metadata["quotechar"] = '"'
                    metadata["first_line"] = metadata["first_line"].replace('"', '')

                # Set delimiter character and generate list of fields
                if metadata["first_line"].find("|") != -1:
                    metadata["field_names"] = metadata["first_line"].split("|")
                    if len(metadata["field_names"]) > 4:
                        metadata["delimiter"] = "|"
                elif metadata["first_line"].find(",") != -1:
                    metadata["field_names"] = metadata["first_line"].split(",")
                    if len(metadata["field_names"]) > 4:
                        metadata["delimiter"] = ","

                # Handle cases where there is something weird in the file with field names
                if "field_names" in metadata.keys():
                    if len(metadata["field_names"]) > 30 or len(metadata["first_line"]) > 1000:
                        metadata["First Line Extraction Error"] = \
                            f"Line Length - {len(metadata['first_line'])}, " \
                            f"Number Fields - {len(metadata['field_names'])}"
                        del metadata["field_names"]
                        del metadata["first_line"]
                        del metadata["delimiter"]
            except Exception as e:
                metadata["First Line Extraction Error"] = str(e)

        # Pre-flag whether or not we think this is a processable route to collection records
        if len(explicit_source_files) > 0:
            if file["pathOnDisk"] in explicit_source_files:
                metadata["Processable Route"] = True
            else:
                metadata["Processable Route"] = False
        else:
            metadata["Processable Route"] = True

            # Set processable route to false if we encountered a problem reading a text file
            if "First Line Extraction Error" in metadata.keys():
                metadata["Processable Route"] = False
            # Set processable route to false if this is one of the old collection metadata files
            if file["name"] == "metadata.xml" and r_file.headers["Content-Type"] == "application/xml":
                metadata["Processable Route"] = False
            # Set processable route to false if this is an FGDC XML metadata file
            if r_file.headers["Content-Type"] == "application/fgdc+xml":
                metadata["Processable Route"] = False
            # Flag other types of "application" files as not processable at this point
            if r_file.headers["Content-Type"].split("/")[0] == "application" and r_file.headers["Content-Type"].split("/")[1] != "xml":
                metadata["Processable Route"] = False

        evaluated_file_list.append(metadata)

    return evaluated_file_list


def link_meta(collection_id, webLinks):
    """
    Retrieve and package metadata for a set of web links (expects list of webLink objects in ScienceBase Item format;
    or something similar since that structure is pretty simple).

    Args:
        collection_id: (str) ScienceBase ID of the collection.
        webLinks: (list of dicts) webLink objects in a list from the ScienceBase collection item

    Note: In the workflow, this function is used to pre-build the ndc_weblinks, a database collection where link
    information is cached for later processing. It processes through the webLink objects in the list and adds
    information based on retrieving and examining the webLink via its uri.
    """

    # Set up evaluated webLink list
    evaluated_link_list = list()

    for link in webLinks:
        # Copy in all source properties
        metadata = link

        # Add context
        metadata["collection_id"] = collection_id
        metadata["Date Checked"] = datetime.utcnow().isoformat()
        metadata["HTTP Response"] = dict()

        # We go ahead and test every link to at least get a current status code
        try:
            response = requests.get(link["uri"])
            metadata["HTTP Response"]["status_code"] = response.status_code
        except Exception as e:
            metadata["HTTP Response Error"] = str(e)


        # For now, we're just teasing out and doing something special with WAF links
        # These are explicitly identified for the few cases as WAF type link
        if "type" in link.keys() and "response" in locals():
            if link["type"] == "WAF" and response.status_code == 200:
                metadata["HTTP Response"]["apparent_encoding"] = response.apparent_encoding
                for k,v in response.headers.items():
                    metadata["HTTP Response"][k] = v
                metadata["WAF Listing"] = list_waf(url=link["uri"], response=response)

        evaluated_link_list.append(metadata)

    return evaluated_link_list


def nggdpp_xml_to_dicts(file_meta):
    """
    Retrieve a processable NGGDPP XML file and convert it's contents to a list of dictionaries

    Args:
        file_meta: (dict) File metadata structure created with the file_meta() function

    Note: The expected output of this function is a list of dicts (each record in the dataset). A single dict
    returned indicates that an error occurred, and that is supplied in the return.
    """

    if file_meta["Content-Type"] != "application/xml":
        output = {"Error": "File is not application/xml"}
        return output
    else:
        # Read the XML file data and convert to a dictionary for ease of use
        xmlData = requests.get(file_meta["url"]).text
        dictData = xmltodict.parse(xmlData, dict_constructor=dict)

        # Handle the corner case (come up with a more elegant way of detecting XML structure
        if 'samples' in dictData.keys():
            output = dictData['samples']['sample']
        elif 'historictopomaps' in dictData.keys():
            output = dictData['historictopomaps']['topomap']
        else:
            output = {"Error": "Could not find 'samples' or 'historictopos' in XML file"}

        if not isinstance(output, list):
            output = {"Error": "Output from process is not a list"}
        else:
            if len(output) == 0:
                output = {"Error": "File processing produced an empty list of records"}
            else:
                if not isinstance(output[0], dict):
                    output = {"Error": "First element of source list of dicts is not a dict"}

        return output


def nggdpp_text_to_dicts(file_meta):
    """
    Retrieve a processable NGGDPP text file and convert it's contents to a list of dictionaries

    Args:
        file_meta: (dict) File metadata structure created with the file_meta() function

    Note: The expected output of this function is a list of dicts (each record in the dataset). A single dict
    returned indicates that an error occurred, and that is supplied in the return.
    """

    if file_meta["Content-Type"] == "application/xml":
        # redirect to the xml processor
        return nggdpp_xml_to_dicts(file_meta)
    else:
        if "delimiter" in file_meta.keys():
            df_file = pd.read_csv(file_meta["url"],
                                  sep=file_meta["delimiter"], encoding=file_meta["encoding"])
        else:
            try:
                df_file = pd.read_csv(file_meta["url"],
                                      sep="|", encoding=file_meta["encoding"])

                # If that only produces a single column dataframe, we try a comma.
                if len(list(df_file)) == 1:
                    df_file = pd.read_csv(file_meta["url"],
                                          sep=",", encoding=file_meta["encoding"])
            except Exception as e:
                # If that bombs, it's usually an encoding problem somewhere in the content
                # Try latin1 encoding as a last result which introduces issues wherever the offending characters are.
                try:
                    df_file = pd.read_csv(file_meta["url"], sep="|", encoding="latin1")
                    if len(list(df_file)) == 1:
                        df_file = pd.read_csv(file_meta["url"], sep=",", encoding="latin1")
                except Exception as e:
                    output = {"Error": "Problem reading text file with Pandas", "Exception": str(e)}
                    return output

        # Drop all the rows with empty cells.
        df_file = df_file.dropna(how="all")

        # Drop all unnamed columns
        df_file.drop(df_file.columns[df_file.columns.str.contains('unnamed', case=False)], axis=1)

        # Outputting the dataframe to JSON and then loading to a dictionary makes for the cleanest eventual GeoJSON.
        json_file = df_file.to_json(orient="records")

        # Set source list to a dictionary from the JSON construct
        output = json.loads(json_file)

        if not isinstance(output, list):
            output = {"Error": "Output from process is not a list"}
        else:
            if len(output) == 0:
                output = {"Error": "File processing produced an empty list of records"}
            else:
                if not isinstance(output[0], dict):
                    output = {"Error": "First element of source list of dicts is not a dict"}

        return output


def nggdpp_record_list_to_geojson(record_source, file_meta, source_metadata_summary):
    """
    Take a list of dictionaries containing NGGDPP records, convert to GeoJSON features, and return a Feature Collection

    Args:
        record_source: (list of dicts) List of NGGDPP records returned from nggdpp_text_to_dicts() or
        nggdpp_xml_to_dicts()
        file_meta: (dict) File metadata structure created with the file_meta() function
        source_metadata_summary: (dict) Summary of source collection metadata properties
        from ndcCollection.collection_metadata_summary()

    Note: The expected output of this function is a dictionary containing the original file_meta structure,
    a GeoJSON feature collection, and a processing log containing a report of the process. In the workflow, the
    processing log is added to the collection record in the ndc_log data store, and the features from the
    feature collection are added to their own collection (using the collection ID as name) and piped into
    ElasticSearch for use.
    """

    # Set up a processing report to record what happens
    file_meta["Processing Report"] = dict()
    file_meta["Processing Report"]["Number of Errors"] = 0

    feature_list = []
    for p in record_source:
        # Lower-casing the keys from the original data makes things simpler
        p = {k.lower(): v for k, v in p.items()}

        # Add the collection ID for reference
        p["collection_id"] = file_meta["collection_id"]

        # Infuse file properties
        p["source_file"] = file_meta["url"]
        p["source_file_uploaded"] = file_meta["dateUploaded"]
        p["build_from_source_date"] = datetime.utcnow().isoformat()

        # Infuse collection-level metadata properties
        for k, v in source_metadata_summary.items():
            p[k] = v

        # Set up a processing errors container
        p["processing_errors"] = []

        # For now, I opted to take alternate forms of coordinates and put them into a coordinates property to keep
        # with the same overall processing logic
        if "coordinates" not in p.keys() and ("latitude" in p.keys() and "longitude" in p.keys()):
            p["coordinates"] = f'{p["longitude"]},{p["latitude"]}'

        # Set default empty geometry
        g = Point(None)

        # Try to make point geometry from the coordinates
        # This is where I'm still running into some errors I need to go back and work through
        if "coordinates" in p.keys():
            try:
                if "," in p["coordinates"]:
                    g = build_point_geometry(p["coordinates"])
            except Exception as e:
                this_error = processing_error("geometry",
                                                    f"{e}; {str(p['coordinates'])}; kept empty geometry")
                p["processing_errors"].append(this_error)
                file_meta["Processing Report"]["Number of Errors"] += 1

        # Add feature to list
        feature_list.append(build_ndc_feature(g, p))

    # Add some extra file processing metadata
    file_meta["Processing Report"]["Build From Source Date"] = datetime.utcnow().isoformat()
    file_meta["Processing Report"]["Number of Records"] = len(feature_list)

    # Make a Feature Collection from the feature list
    # We don't really need to do this since it's not how we are storing the data, but it makes this function usable
    # beyond our immediate use case.
    file_meta["Feature Collection"] = FeatureCollection(feature_list)

    return file_meta


def processing_error(property, error_str):
    """
    Build a simple error structure indicating the section of the data model and the error that occurred.

    Args:
        property: (str) Property in the data model where the error was found
        error_str: (str) Error string to record

    Note: This is used in the data processing flow to indicate record level issues with a given property that were
    not fatal but were recorded for later action.
    """

    error = dict()
    error[property] = error_str
    error['DateTime'] = datetime.utcnow().isoformat()

    return error


def process_log_entry(collection_id, source_meta, time_to_run, errors=None):
    """
    Build a process log entry to drop in the process_log data store. Information logged here is for each time
    a source file/waf/etc is processed for a given collection. It records some basic information about what happened
    in the process.

    Args:
        collection_id: (str) Collection identifier
        source_meta: (dict) Data object containing the full metadata for either a file or a waf that was processed
        errors: (list) Possible list of errors that occurred, either fatal or not, in source processing

    Note: Presence of a dateUploaded property in source_meta indicates that the source is a file, and that type
    flag is set appropriately. My intent is to use source_date to help determine if a given source needs to be
    reprocessed if the file from ScienceBase is newer than the latest processing of that file.
    """

    log_entry = dict()

    log_entry["collection_id"] = collection_id
    log_entry["date_stamp"] = datetime.utcnow().isoformat()
    log_entry["time_to_run_process_in_seconds"] = time_to_run

    log_entry["source_meta"] = source_meta

    if "dateUploaded" in source_meta.keys():
        log_entry["source_type"] = "file"
        log_entry["source_date"] = source_meta["dateUploaded"]
    else:
        log_entry["source_type"] = "WAF"

    if errors is not None:
        log_entry["errors"] = errors

    return log_entry


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


def set_env_variables(config_file):
    try:
        env_vars_set = []
        with open(config_file) as f:
            for line in f:
                if '=' not in line:
                    continue
                if line.startswith('#'):
                    continue
                key, value = line.replace('export ', '', 1).strip().split('=', 1)
                os.environ[key] = value
                env_vars_set.append(key)
        f.close()
        return env_vars_set
    except Exception as e:
        return e


def mongodb_client():
    mongo_uri = "mongodb://" + os.environ["MONGODB_USERNAME"] + ":" + os.environ["MONGODB_PASSWORD"] + "@" + os.environ[
        "MONGODB_SERVER"] + "/" + os.environ["MONGODB_DATABASE"]
    client = MongoClient(mongo_uri)
    return client.get_database(os.environ["MONGODB_DATABASE"])


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
