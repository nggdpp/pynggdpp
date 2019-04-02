from datetime import datetime
import dateutil.parser as dt_parser
import json
import sys
import numpy as np
import uuid

import pandas as pd
import requests
from bs4 import BeautifulSoup
import xmltodict
from geojson import Feature, Point, FeatureCollection
from geojson import dumps as geojson_dumps
from gis_metadata.metadata_parser import get_metadata_parser
from gis_metadata.utils import get_supported_props

from .aws import Connect
from .aws import Storage
from .aws import Messaging
from .rest_api import Search
from .serverful import Infrastructure


class Links:
    def __init__(self):
        self.data={}

    def parse_waf(self, url):
        try:
            r = requests.get(url)
        except Exception as e:
            return None


        waf_package = dict()
        waf_package["url"] = r.url
        waf_package["headers"] = r.headers
        waf_package["url_list"] = list()

        soup = BeautifulSoup(r.content, "html.parser")

        if soup.find("pre"):
            processed_list = dict()

            for index, line in enumerate(soup.pre.children):
                if index > 1:
                    try:
                        line_contents = line.get_text()
                    except:
                        line_contents = list(filter(None, str(line).split(" ")))
                    processed_list[index] = line_contents

            for k, v in processed_list.items():
                if k % 2 == 0:
                    if v.split(".")[-1] == "xml":
                        item = dict()
                        item["ndc_file_name"] = v
                        item["ndc_file_url"] = f"{r.url}{v}"
                        item["ndc_file_date"] = dt_parser.parse(
                            f"{processed_list[k + 1][0]} {processed_list[k + 1][1]}").isoformat()
                        item["ndc_file_size"] = processed_list[k + 1][2].replace("\r\n", "")
                        waf_package["url_list"].append(item)

        elif soup.find("table"):
            for index, row in enumerate(soup.table.find_all("tr")):
                if index > 2:
                    item = dict()
                    columns = row.find_all("td")
                    for i, column in enumerate(columns):
                        cell_text = column.get_text().strip()
                        if i == 1:
                            item["ndc_file_name"] = cell_text
                            item["ndc_file_url"] = f"{r.url}{cell_text}"
                        elif i == 2:
                            item["ndc_file_date"] = dt_parser.parse(cell_text).isoformat()
                        elif i == 3:
                            item["ndc_file_size"] = cell_text
                    if "file_name" in item.keys() and item["file_name"].split(".")[-1] == "xml":
                        waf_package["url_list"].append(item)

        return waf_package


class Files:
    def __init__(self):
        self.aws_storage = Storage()
        self.spatial_processor = Spatial()
        self.rest_search = Search()
        self.temporal_processor = Temporal()

    def introspect_nggdpp_xml(self, dict_data):
        introspection_meta = dict()
        top_keys = list(dict_data.keys())
        if len(top_keys) == 1:
            second_keys = list(dict_data[top_keys[0]].keys())
            if isinstance(dict_data[top_keys[0]][second_keys[-1]], list):
                introspection_meta['ndc_record_container_path'] = [top_keys[0], second_keys[-1]]
                introspection_meta['ndc_record_number'] = len(dict_data[top_keys[0]][second_keys[-1]])
                introspection_meta['ndc_field_names'] = list(dict_data[top_keys[0]][second_keys[-1]][0].keys())

        return introspection_meta

    def clean_dict_from_nggdpp_xml(self, file_object):
        meta = {
            "file_url": file_object["ndc_file_url"],
            "file_downloaded": datetime.utcnow().isoformat(),
            "errors": list()
        }

        response = requests.get(file_object["ndc_file_url"])
        source_data = xmltodict.parse(response.text, dict_constructor=dict)

        introspection_meta = self.introspect_nggdpp_xml(source_data)

        meta["accepted_record_number"] = introspection_meta['ndc_record_number']

        xml_tree_top = introspection_meta['ndc_record_container_path'][0]
        xml_tree_next = introspection_meta['ndc_record_container_path'][1]
        recordset = source_data[xml_tree_top][xml_tree_next]
        meta["property_names"] = list(recordset[0].keys())

        recordset = [{k.lower(): v for k, v in i.items()} for i in recordset]
        for item in recordset:
            # Evaluate coordinates information if present
            item.update(self.spatial_processor.introspect_coordinates(item))

            # Evaluate date field if present
            item.update(self.temporal_processor.introspect_date(item))

            # Add in the file and collection metadata properties
            for k, v in file_object.items():
                item.update({k: v})

            # Add the date we indexed this data
            item["ndc_date_file_indexed"] = datetime.utcnow().isoformat()

            # Split datatype values into a list
            if "datatype" in item.keys() and isinstance(item["datatype"], str):
                item["datatype"] = item["datatype"].split(",")

        return {
            "processing_metadata": meta,
            "recordset": recordset
        }

    def clean_dict_from_csv(self, file_object):
        comma_allowed = [
            "title",
            "alternatetitle",
            "abstract",
            "datatype",
            "supplementalinformation",
            "coordinates",
            "alternategeometry"
        ]

        meta = {
            "file_url": file_object["ndc_file_url"],
            "file_delimiter": "|",
            "file_encoding": "ascii",
            "file_downloaded": datetime.utcnow().isoformat(),
            "errors": list()
        }

        sys.stdout = x = ListStream()
        try:
            df = pd.read_csv(
                meta["file_url"],
                delimiter=meta["file_delimiter"],
                encoding=meta["file_encoding"],
                error_bad_lines=False,
                warn_bad_lines=True
            )
        except UnicodeDecodeError:
            meta["file_encoding"] = "latin1"
            df = pd.read_csv(
                meta["file_url"],
                delimiter=meta["file_delimiter"],
                encoding=meta["file_encoding"],
                error_bad_lines=False,
                warn_bad_lines=True
            )
        except Exception as e:
            meta["errors"].append(str(e))
            return meta

        if len(df.columns) == 1:
            meta["file_delimiter"] = ","
            df = pd.read_csv(
                meta["file_url"],
                delimiter=meta["file_delimiter"],
                encoding=meta["file_encoding"],
                error_bad_lines=False,
                warn_bad_lines=True
            )
        sys.stdout = sys.__stdout__

        # Record any error line problems that came up in reading the CSV file to dataframe
        if len(x.data) > 0:
            meta["error_lines"] = x.data

        # Replace values in select columns where there are multiple commas with None
        for column in [c for c in df.columns if c.lower() not in comma_allowed]:
            df[column].replace(r'(,)\1*', None, inplace=True, regex=True)

        # Replace whitespace with None
        df.replace({r'\s+': None}, regex=True, inplace=True)

        # Replace multiple commas with None
        df.replace({r'(,)\1*': None}, regex=True, inplace=True)

        # Drop any columns with all NaN values
        df.dropna(axis=1, how="all", inplace=True)

        # Drop any rows with all NaN values
        df.dropna(axis=0, how="all", inplace=True)

        # Make lower case column names and get rid of pesky commas
        df.rename(columns={col: col.replace(',', '').lower() for col in df.columns}, inplace=True)
        df.rename(columns={col: col.replace('.', '_') for col in df.columns}, inplace=True)
        df.rename(columns={"": str(uuid.uuid4()), " ": str(uuid.uuid4())}, inplace=True)

        # Fix dates for future indexing
        for date_field in ["date", "datasetreferencedate"]:
            if date_field in df.columns:
                df[f"{date_field}_original"] = df[date_field]
                print(self.temporal_processor.cleanup_date(df[date_field]))
                df[date_field].apply(self.temporal_processor.cleanup_date)
                try:
                    pd.to_datetime(df[date_field])
                except Exception as e:
                    meta["errors"].append(str(e))

        # Try to parse out and process coordinates
        if ("latitude" in df.columns and "longitude" in df.columns) and "coordinates" not in df.columns:
            df.coordinates = list(zip(df.longitude, df.latitude))

        # Replace nan with None
        df.replace({np.nan: None}, inplace=True)

        # Add summary metadata
        meta["accepted_columns"] = list(df.columns)
        meta["accepted_record_number"] = len(df)

        # Add in spatial processing
        recordset = list()
        for item in df.to_dict(orient="records"):
            for k, v in file_object.items():
                item.update({k: v})
            recordset.append(self.spatial_processor.introspect_coordinates(item))

            # Add the date we indexed this data
            item["ndc_date_file_indexed"] = datetime.utcnow().isoformat()

        return {
            "processing_metadata": meta,
            "recordset": recordset
        }

    def ndc_item_from_metadata(self, file_object):
        meta = {
            "file_url": file_object["ndc_file_url"],
            "file_downloaded": datetime.utcnow().isoformat(),
            "errors": list()
        }

        response = requests.get(file_object["ndc_file_url"])

        feature_data = self.spatial_processor.feature_from_metadata(response.text)

        item_record = feature_data["properties"]
        if feature_data["geometry"]["type"] == "Point":
            item_record["coordinates"] = str(feature_data["geometry"]["coordinates"])

        meta["property_names"] = list(item_record.keys())

        if "coordinates" in item_record.keys():
            item_record = self.spatial_processor.introspect_coordinates(item_record)

        for k, v in file_object.items():
            item_record.update({k: v})

        item_record["ndc_date_file_indexed"] = datetime.utcnow().isoformat()

        meta["accepted_record_number"] = 1

        return {
            "processing_metadata": meta,
            "recordset": [item_record]
        }

class Spatial:
    def __init__(self):
        self.data={}

    def introspect_coordinates(self, item):
        if "ndc_processing_notices" not in item.keys():
            item["ndc_processing_notices"] = list()

        if "coordinates" not in item.keys():
            if ("latitude" in item.keys() and "longitude" in item.keys()):
                item["coordinates"] = f'{item["longitude"]},{item["latitude"]}'

        if "coordinates" in item.keys() and item["coordinates"] is not None:
            try:
                if "," in item["coordinates"]:
                    try:
                        item["ndc_location"] = \
                            Point((float(item["coordinates"].split(',')[0]), float(item["coordinates"].split(',')[1])))
                        item["ndc_geopoint"] = {
                            "lon": float(item["coordinates"].split(',')[0]),
                            "lat": float(item["coordinates"].split(',')[1])
                        }
                    except:
                        pass
            except Exception as e:
                item["ndc_processing_notices"].append(
                    {
                        "error": e,
                        "info": f"{str(item['coordinates'])}; kept empty geometry"
                    }
                )
        else:
            item["ndc_processing_notices"].append(
                {
                    "error": "Null Coordinates",
                    "info": "Could not determine location from data"
                }
            )

        return item

    def nggdpp_recordset_to_feature_collection(self, recordset):
        feature_list = []

        for record in recordset:
            p = {k.lower(): v for k, v in record.items()}
            p["ndc_processing_errors"] = list()
            p["ndc_processing_errors_number"] = 0

            if "coordinates" not in p.keys():
                if ("latitude" in p.keys() and "longitude" in p.keys()):
                    p["coordinates"] = f'{p["longitude"]},{p["latitude"]}'

            g = Point(None)
            if "coordinates" in p.keys() and p["coordinates"] is not None:
                try:
                    if "," in p["coordinates"]:
                        try:
                            g = Point((float(p["coordinates"].split(',')[0]), float(p["coordinates"].split(',')[1])))
                        except:
                            pass
                except Exception as e:
                    p["ndc_processing_errors"].append(
                        {
                            "error": e,
                            "info": f"{str(p['coordinates'])}; kept empty geometry"
                        }
                    )
                    p["ndc_processing_errors_number"]+=1

            feature_list.append(Feature(geometry=g, properties=p))

        return FeatureCollection(feature_list)

    def feature_from_metadata(self, meta_doc):
        p = dict()

        # Parse the metadata XML using the gis_metadata tools
        parsed_metadata = get_metadata_parser(meta_doc)

        # Add any and all properties that aren't blank (ref. gis_metadata.utils.get_supported_props())
        #for prop in get_supported_props():
        for prop in get_supported_props():
            v = parsed_metadata.__getattribute__(prop)
            if len(v) > 0:
                p[prop.lower()] = v

        # Process geometry if a bbox exists
        p['coordinates_point'] = {}

        if len(parsed_metadata.bounding_box) > 0:
            # Pull out bounding box elements
            east = float(parsed_metadata.bounding_box["east"])
            west = float(parsed_metadata.bounding_box["west"])
            south = float(parsed_metadata.bounding_box["south"])
            north = float(parsed_metadata.bounding_box["north"])

            # Record a couple processable forms of the BBOX for later convenience
            p['coordinates_geojson'] = [[west, north], [east, north], [east, south], [west, south]]
            p['coordinates_wkt'] = [[(west, north), (east, north), (east, south), (west, south), (west, north)]]

            p['coordinates_point']['coordinates'] = f"{east},{south}"
            p['coordinates_point']['method'] = "bbox corner"
            p['ndc_geopoint'] = f"{east},{south}"
        else:
            p['coordinates_point']['coordinates'] = None
            p['coordinates_point']['method'] = "no processable geometry"

        # Generate the point geometry
        g = self.build_point_geometry(p["coordinates_point"]["coordinates"])

        # Build the GeoJSON feature from the geometry with its properties
        f = self.build_ndc_feature(g, p)

        # Convert the geojson object to a standard dict
        f = json.loads(geojson_dumps(f))

        return f

    def build_point_geometry(self, coordinates):
        if coordinates == "0,0":
            return Point(None)

        probable_lng, probable_lat = map(float, coordinates.split(","))

        # Reverse coordinates if reasonable
        if int(probable_lat) not in range(-90, 90) and int(probable_lng) in range(-90, 90):
            lat = float(probable_lng)
            lng = float(probable_lat)
        else:
            lat = float(probable_lat)
            lng = float(probable_lng)

        try:
            pointGeometry = Point((lng, lat))
            if not pointGeometry.is_valid:
                pointGeometry = Point(None)
        except:
            pointGeometry = Point(None)

        return pointGeometry

    def build_ndc_feature(self, geom, props):
        ndcFeature = Feature(geometry=geom, properties=props)
        return ndcFeature

    def build_ndc_feature_collection(self, feature_list):
        return FeatureCollection(feature_list)


class Temporal:
    def __init__(self):
        data = {}

    def cleanup_date(self, datestr):
        try:
            yield dt_parser.parse(datestr, dayfirst=True)
        except (ValueError, TypeError) as e:
            return f"Exception {e} on unhandled date {datestr}"

    def introspect_date(self, item):
        if "ndc_processing_notices" not in item.keys():
            item["ndc_processing_notices"] = list()

        if "date" not in item.keys() or item["date"] is None:
            return item
        else:
            date_string = item["date"]
            delimiter = None
            for delim in ["-", "/"]:
                if delim in date_string:
                    delimiter = delim
                    break

            if delimiter is not None:
                date_parts = ["01" if x == "00" else x for x in date_string.split(delimiter)]
                date_string = delimiter.join(date_parts)

            try:
                item["date"] = dt_parser.parse(date_string)
            except ValueError as e:
                item["ndc_processing_notices"].append({
                    "error": str(e),
                    "info": "Date string could not be parsed, moved value to 'date_string' property"
                })
                item["date_string"] = item["date"]
                del item["date"]

            return item


class Log:
    def __init__(self):
        aws_connect = Connect()
        self.aws_messaging = Messaging()
        self.es = aws_connect.elastic_client()
        self.serverful_infrastructure = Infrastructure()

    def log_process_step(self,
                         identifier,
                         entry_type,
                         log,
                         context="serverless",
                         message_queue_packet=None,
                         index="processing_log",
                         doc_type="log_entry",
                         source_file=None,
                         source_function=None
                         ):
        log_entry = {
            "identifier": identifier,
            "process_date": datetime.utcnow().isoformat(),
            "source_file": source_file,
            "source_function": source_function,
            "entry_type": entry_type,
            "log_entry": log
        }
        if context == "serverless":

            self.es.index(
                index=index,
                doc_type=doc_type,
                body=log_entry
            )
            if message_queue_packet is not None:
                self.aws_messaging.post_message(
                    message_queue_packet["message_queue"],
                    message_queue_packet["identifier"],
                    log
                )
        elif context == "serverful":
            processing_log = self.serverful_infrastructure.connect_mongodb(collection="processing_log")
            processing_log.insert_one(log_entry)
            if message_queue_packet is not None:
                passon_db = self.serverful_infrastructure.connect_mongodb(collection=message_queue_packet["message_queue"])
                passon_db.insert_one(log)

        return log_entry


class General:
    def __init__(self):
        self.data={}

    def build_ndc_metadata(self, context):
        context_meta = {}
        for section, content in context.items():
            for k, v in content.items():
                if k.split("_")[0] != "ndc":
                    k = f"ndc_{k}"
                context_meta.update({k: v})

        if "ndc_pathOnDisk" in context_meta.keys():
            context_meta["ndc_s3_file_key"] = f"{context_meta['ndc_pathOnDisk']}/{context_meta['ndc_name']}"
        elif "ndc_file_url" in context_meta.keys():
            context_meta["ndc_s3_file_key"] = url_to_s3_key(context_meta["ndc_file_url"])

        context_meta["ndc_date_record_created"] = datetime.utcnow().isoformat()
        return context_meta


class ListStream:
    def __init__(self):
        self.data = []

    def write(self, s):
        self.data.append(s)


