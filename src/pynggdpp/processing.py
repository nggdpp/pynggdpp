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
import json
import os
from pymongo import MongoClient
import pandas as pd
from pynggdpp import collection as ndcCollection


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
    """
    Use BeautifulSoup to retrieve a web page listing of links for harvesting. Most web accessible folders present
    themselves as an HTML response, making this a reasonable way of putting together a link listing.

    :param url: (str) The URL to the WAF, this is requested if a response object is not provided.
    :param response: (HTTP response object from requests) An HTTP response provided from another function.
    :param ext: (str) File extension to use in filtering the list of links.
    :return: Returns a simple listing of URLs

    Note: This is super simplistic at the moment, and I need to make it a more sophisticated function as we have
    more types of harvestable places to parse.
    """

    # Get the URL if needed
    if response is None:
        page = requests.get(url).text
    else:
        page = response.text

    # Parse the response text contents with the HTML parser
    soup = BeautifulSoup(page, 'html.parser')

    # Return the list of URLs filtered according to extension
    return [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]


def build_ndc_feature_collection(feature_list):
    return FeatureCollection(feature_list)


def inspect_response(response):
    """
    Build packet of information from HTTP response to add to file or link metadata

    :param response: HTTP response object from requests
    :return: Dictionary containing packaged response properties
    """

    response_meta = dict()

    # Add header information
    for k, v in response.headers.items():
        response_meta[k] = v

    # Create basic classification of file type for convenience
    response_meta["file-class"] = "Unknown"
    if "Content-Type" in response_meta.keys():
        if response_meta["Content-Type"].find("/") != -1:
            response_meta["file-class"] = response_meta["Content-Type"].split("/")[0]

    return response_meta


def inspect_response_content(response, response_meta=None):
    """
    Runs a file introspection process to evaluate file contents and build a set of properties for further evaluation.

    :param response: HTTP response object from requests
    :param response_meta: Dictionary containing the basic response metadata; required for this function to run but
    assembled here if not provided
    :return: Dictionary containing results of file introspection
    """

    introspection_meta = dict()

    # Grab the basic response metadata if not provided
    if response_meta is None:
        response_meta = inspect_response(response)

    # Pull out the first line of text file responses and analyze for content
    if response_meta["file-class"] == "text":
        if response.encoding is None:
            response.encoding = 'latin1'

        for index,line in enumerate(response.iter_lines(decode_unicode=True)):
            if index > 0:
                break
            if line:
                introspection_meta["first_line"] = line

        introspection_meta["encoding"] = response.encoding

        # Check for quote characters
        if introspection_meta["first_line"].find('"') != -1:
            introspection_meta["quotechar"] = '"'
            introspection_meta["first_line"] = introspection_meta["first_line"].replace('"', '')

        # Set delimiter character and generate list of fields
        if introspection_meta["first_line"].find("|") != -1:
            introspection_meta["field_names"] = introspection_meta["first_line"].split("|")
            if len(introspection_meta["field_names"]) > 4:
                introspection_meta["delimiter"] = "|"
        elif introspection_meta["first_line"].find(",") != -1:
            introspection_meta["field_names"] = introspection_meta["first_line"].split(",")
            if len(introspection_meta["field_names"]) > 4:
                introspection_meta["delimiter"] = ","

        # Handle cases where there is something weird in the file with field names
        if "field_names" in introspection_meta.keys():
            if len(introspection_meta["field_names"]) > 30 or len(introspection_meta["first_line"]) > 1000:
                introspection_meta["First Line Extraction Error"] = \
                    f"Line Length - {len(introspection_meta['first_line'])}, " \
                    f"Number Fields - {len(introspection_meta['field_names'])}"
                del introspection_meta["field_names"]
                del introspection_meta["first_line"]
                del introspection_meta["delimiter"]

    elif response_meta["file-class"] == "application":
        introspection_meta["message"] = "Application file processing not yet implemented"

    else:
        introspection_meta["message"] = "Response contained nothing to process"

    return introspection_meta


def check_processable(explicit_source_files, source_meta, response_meta, content_meta):
    """
    Evaluates source and processed metadata for file content and determines whether the content is processable.

    :param explicit_source_files: List of explicitly flagged source files (pathOnDisk) evaluated for a list of
    ScienceBase Files
    :param source_meta: Source metadata structure (ScienceBase File object)
    :param response_meta: Metadata from the HTTP response for the file object from inspect_response()
    :param content_meta: Metadata from the file content introspection from inspect_response_content()
    :return: Dictionary containing process parameters
    """
    process_meta = dict()

    # Pre-flag whether or not we think this is a processable route to collection records
    if len(explicit_source_files) > 0:
        if source_meta["pathOnDisk"] in explicit_source_files:
            process_meta["Processable Route"] = True
        else:
            process_meta["Processable Route"] = False
    else:
        process_meta["Processable Route"] = True

        # Set processable route to false if we encountered a problem reading a text file
        if "First Line Extraction Error" in content_meta.keys():
            process_meta["Processable Route"] = False
        # Set processable route to false if this is one of the old collection metadata files
        if source_meta["name"] == "metadata.xml" and response_meta["Content-Type"] == "application/xml":
            process_meta["Processable Route"] = False
        # Set processable route to false if this is an FGDC XML metadata file
        if response_meta["Content-Type"] == "application/fgdc+xml":
            process_meta["Processable Route"] = False
        # Flag other types of "application" files as not processable at this point
        if response_meta["Content-Type"].split("/")[0] == "application":
            if response_meta["Content-Type"].split("/")[1] != "xml":
                process_meta["Processable Route"] = False

    return process_meta


def file_meta(collection_id, files=None):
    """
     Retrieve and package metadata for a set of file (expects list of file objects in ScienceBase Item format).

    :param collection_id: (str) ScienceBase ID of the collection.
    :param files: (list of dicts) file objects in a list from the ScienceBase collection item
    :return: (list of dicts) file metadata with value-added processing information

    In the workflow, this function is used to pre-build ndc_files, a database collection
    where file information is cached for later processing. It processes through the file objects in the list and
    adds information based on retrieving and examining the file via its url.
    """

    # Put a single file object into a list
    if isinstance(files, dict):
        files = [files]

    # Get files if not provided
    if files is None:
        collections = ndcCollection.ndc_get_collections(collection_id=collection_id, fields="title,contacts,files")
        if collections is None or len(collections) == 0:
            return {"Error": "Cannot run without files being provided"}
        collection = collections[0]
        files = collection["files"]

    # Check for explicit Source Data flagging in the collection
    explicit_source_files = [f["pathOnDisk"] for f in files if "title" in f.keys() and f["title"] == "Source Data"]

    # Set up evaluated file list
    evaluated_file_list = list()

    for file in files:
        metadata = dict()

        # Copy in all source properties
        metadata["source_meta"] = file

        # Add context
        metadata["collection_id"] = collection_id
        metadata["Date Checked"] = datetime.utcnow().isoformat()

        # Retrieve the file to check things out
        response = requests.get(file["url"], stream=True)

        # Inspect the response and return metadata
        metadata["response_meta"] = inspect_response(response)

        # Inspect the response content and return metadata
        metadata["content_meta"] = inspect_response_content(response, response_meta=metadata["response_meta"])

        # Determine whether or not the file route is a processable one for the NDC
        metadata["process_meta"] = check_processable(explicit_source_files,
                                                     metadata["source_meta"],
                                                     metadata["response_meta"],
                                                     metadata["content_meta"])

        # Add collection metadata summary for processing convenience
        if "collection" in locals():
            metadata["collection_meta"] = ndcCollection.collection_metadata_summary(collection=collection)
        else:
            metadata["collection_meta"] = ndcCollection.collection_metadata_summary(collection_id=collection_id)

        evaluated_file_list.append(metadata)

    return evaluated_file_list


def link_meta(collection_id, webLinks=None):
    """
    Processes the web links of a collection to determine their potential as a route for collection items and set
    things up for processing.

    :param collection_id: (str) ScienceBase Item ID of the collection
    :param webLinks: (list of dicts) List of webLink objects in the ScienceBase format; if not supplied, the function
    will execute a function to retrieve the webLinks for a collection
    :return: In the workflow, this function is used to pre-build the ndc_weblinks, a database collection where link
    information is cached for later processing. It processes through the webLink objects in the list and adds
    information based on retrieving and examining the webLink via its uri.
    """

    # Set up an error container. If the function return is a single dict instead of a list of dicts, that signals
    # a problem
    error_container = dict()
    error_container["collection_id"] = collection_id

    # Put a single webLink object into a list
    if isinstance(webLinks, dict):
        webLinks = [webLinks]

    # Get files if not provided
    if webLinks is None:
        collections = ndcCollection.ndc_get_collections(collection_id=collection_id, fields="title,contacts,webLinks")
        if collections is None or len(collections) == 0:
            error_container["error"] = "Cannot run without files being provided"
            return error_container

        if "webLinks" not in collections[0].keys():
            error_container["error"] = "No webLinks found in the collection to evaluate"
            return error_container

        collection = collection[0]
        webLinks = collection["webLinks"]

    # Set up evaluated webLink list
    evaluated_link_list = list()

    for link in webLinks:
        metadata = dict()

        # Copy in all source properties
        metadata["source_meta"] = link

        # Add context
        metadata["collection_id"] = collection_id
        metadata["Date Checked"] = datetime.utcnow().isoformat()

        # Set up the response container
        metadata["response_meta"] = dict()

        # We go ahead and test every link to at least get a current status code
        try:
            response = requests.get(link["uri"])
            metadata["response_meta"]["status_code"] = response.status_code
        except Exception as e:
            metadata["response_meta"]["error"] = str(e)

        # For now, we're just teasing out and doing something special with WAF links
        # These are explicitly identified for the few cases as WAF type link
        if "response" in locals() and response.status_code == 200:
            if "type" in link.keys() and link["type"] == "WAF":
                metadata["response_meta"]["apparent_encoding"] = response.apparent_encoding
                for k,v in response.headers.items():
                    metadata["response_meta"][k] = v
                metadata["content_meta"] = dict()
                metadata["content_meta"]["waf_links"] = list_waf(url=link["uri"], response=response)

        # Add collection metadata summary for processing convenience
        if "collection" in locals():
            metadata["collection_meta"] = ndcCollection.collection_metadata_summary(collection=collection)
        else:
            metadata["collection_meta"] = ndcCollection.collection_metadata_summary(collection_id=collection_id)

        evaluated_link_list.append(metadata)

    return evaluated_link_list


def nggdpp_xml_to_dicts(file_meta):
    """
    Retrieve a processable NGGDPP XML file and convert it's contents to a list of dictionaries

    :param file_meta: (dict) File metadata structure created with the file_meta() function
    :return: The expected output of this function is a list of dicts (each record in the dataset). A single dict
    returned indicates that an error occurred, and that is supplied in the return.
    """

    if file_meta["response_meta"]["Content-Type"] != "application/xml":
        output = {"Error": "File is not application/xml"}
        return output
    else:
        # Read the XML file data and convert to a dictionary for ease of use
        xmlData = requests.get(file_meta["source_meta"]["url"]).text
        dictData = xmltodict.parse(xmlData, dict_constructor=dict)

        # Handle the corner case (come up with a more elegant way of detecting XML structure
        if 'samples' in dictData.keys():
            output = dictData['samples']['sample']
        elif 'historictopomaps' in dictData.keys():
            output = dictData['historictopomaps']['topomap']
        elif 'aerial' in dictData.keys():
            output = dictData['aerial']['aerial']
        else:
            output = {"Error": "Could not find processable items in XML file"}

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

    :param file_meta: (dict) File metadata structure created with the file_meta() function
    :return: The expected output of this function is a list of dicts (each record in the dataset). A single dict
    returned indicates that an error occurred, and an error message is included in the return.
    """

    if file_meta["response_meta"]["file-class"] != "text":
        output = {"Error": "Cannot process non-text files at this time"}
    else:
        if "delimiter" in file_meta["content_meta"].keys():
            try:
                df_file = pd.read_csv(file_meta["source_meta"]["url"],
                                      sep=file_meta["content_meta"]["delimiter"],
                                      encoding=file_meta["content_meta"]["encoding"])
            except Exception as e:
                df_file = pd.read_csv(file_meta["source_meta"]["url"],
                                      sep=file_meta["content_meta"]["delimiter"],
                                      encoding="latin1")
            finally:
                output = {"Error": "Problem reading text file with Pandas"}
                return output

        else:
            try:
                df_file = pd.read_csv(file_meta["source_meta"]["url"],
                                      sep="|", encoding=file_meta["content_meta"]["encoding"])

                # If that only produces a single column dataframe, we try a comma.
                if len(list(df_file)) == 1:
                    df_file = pd.read_csv(file_meta["source_meta"]["url"],
                                          sep=",", encoding=file_meta["content_meta"]["encoding"])
            except Exception as e:
                output = {"Error": "Problem reading text file with Pandas", "Exception": str(e)}
                return output

        if "df_file" in locals():
            # Drop all the rows with empty cells.
            df_file = df_file.dropna(how="all")

            # Drop all unnamed columns
            df_file.drop(df_file.columns[df_file.columns.str.contains('unnamed', case=False)], axis=1)

            # Outputting the dataframe to JSON and then loading to a dictionary makes for the cleanest eventual GeoJSON.
            json_file = df_file.to_json(orient="records")

            # Set source list to a list of dictionaries from the JSON construct
            output = json.loads(json_file)

        return output


def nggdpp_record_list_to_geojson(record_source, file_meta):
    """
    Take a list of dictionaries containing NGGDPP records, convert to GeoJSON features, and return a Feature Collection

    :param record_source: List of dictionaries from either file processing or WAF processing
    :param file_meta: File metadata structure from the ndc_files pre-processing
    :return: Note: The expected output of this function is a dictionary containing the original file_meta structure,
    a GeoJSON feature collection, and a processing log containing a report of the process. In the workflow, the
    processing log is added to the collection record in the ndc_log data store, and the features from the
    feature collection are added to their own collection (using the collection ID as name) and piped into
    ElasticSearch for use.
    """

    # Set up a processing report to record what happens
    processing_meta = dict()
    processing_meta["collection_id"] = file_meta["collection_id"]
    processing_meta["source_file_url"] = file_meta["source_meta"]["url"]
    processing_meta["source_file_pathOnDisk"] = file_meta["source_meta"]["pathOnDisk"]
    processing_meta["source_file_dateUploaded"] = file_meta["source_meta"]["dateUploaded"]
    processing_meta["source_file_content-type"] = file_meta["response_meta"]["Content-Type"]
    processing_meta["Number of Errors"] = 0

    feature_list = []
    for p in record_source:
        # Lower-casing the keys from the original data makes things simpler
        p = {k.lower(): v for k, v in p.items()}

        # Infuse file properties
        p["source_file"] = file_meta["source_meta"]["url"]
        p["source_file_uploaded"] = file_meta["source_meta"]["dateUploaded"]

        # Add date this record was produced from source
        p["build_from_source_date"] = datetime.utcnow().isoformat()

        # Infuse collection-level metadata properties
        p["collection_id"] = file_meta["collection_id"]
        for k, v in file_meta["collection_meta"].items():
            p[k] = v

        # Set up a processing errors container
        p["processing_errors"] = []

        # For now, I opted to take alternate forms of coordinates and put them into a coordinates property to keep
        # with the same overall processing logic
        if "coordinates" not in p.keys():
            if ("latitude" in p.keys() and "longitude" in p.keys()):
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
                processing_meta["Number of Errors"] += 1

        # Add feature to list
        feature_list.append(build_ndc_feature(g, p))

    # Add some extra file processing metadata
    processing_meta["processed_date"] = datetime.utcnow().isoformat()
    processing_meta["record_number"] = len(feature_list)

    # Make a Feature Collection from the feature list
    # We don't really need to do this since it's not how we are storing the data, but it makes this function usable
    # beyond our immediate use case.
    processing_package = dict()
    processing_package["processing_meta"] = processing_meta
    processing_package["Feature Collection"] = FeatureCollection(feature_list)

    return processing_package


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


def process_log_entry(collection_id, processing_meta, errors=None):
    """
    Build a process log entry to drop in the process_log data store. Information logged here is for each time
    a source file/waf/etc is processed for a given collection. It records some basic information about what happened
    in the process.

    :param collection_id: (str) Collection identifier
    :param source_meta: (dict) processing metadata packet
    :param errors: (list of dicts) Any errors that came up in processing
    :return: Returns a log entry packaged up and ready to record
    """

    log_entry = dict()

    log_entry["collection_id"] = collection_id
    log_entry["date_stamp"] = datetime.utcnow().isoformat()
    log_entry["processing_meta"] = processing_meta

    if errors is not None:
        log_entry["errors"] = errors

    return log_entry


def ndc_collection_from_waf(waf_url, link_list=None):

    if link_list is None:
        link_list = list_waf(waf_url)

    feature_list = []

    for link in link_list:
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
