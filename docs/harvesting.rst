.. _harvesting:

Collection Harvesting
*********************
In order to efficiently query across disparate collections from many different contributing organizations, the National Digital Catalog harvests information about the items/artifacts in collections using different methods, evaluates incoming records for opportunities to add value through data fusion, and assembles the data together into a set of search indexes optimized for query use. Collection harvesting is controlled by metadata attributes on the collection records as read through the ScienceBase API - either files attached to those items or web links indicating a particular type of source. As we evolve the system toward standardized metadata, we will work solely with distribution metadata describing particularly ways that the digital representation/inventory for a given collection is distributed and available for processing.

Harvesting Methods
==================
The first generation NDC was almost entirely based on files of various types uploaded to ScienceBase and then processed through some built-in tools for file processing through the ScienceBase user interface that were originally built for the NDC and then applied to other use cases. On board files remain the primary mode of providing collection inventories, but we will begin shifting to more of a "pull" method where collection metadata provide a link to a downloadable file as opposed to a "push" method requiring someone to login and upload a file (though, file uploads will still be supported for some time).

In the harvesting process, we are transforming source collection item data from whatever form it is presented into a document data model (NoSQL) that conforms with our overall architecture centered on MongoDB and ElasticSearch, allows for wide variability across source schemas, and supports the infusion of additional value-added properties. The approach is to simply pull in all discrete properties found in each source file or service as they stand without doing any correction or transformation at the initial stage. A small amount of data fusion happens at the harvesting stage to record a few useful details available at the point of harvest.

The following are the value-added contextual properties added to each record as they are harvested and built into a GeoJSON feature. These properties are placed into an ndc_meta object within GeoJSON properties.

* file_url - A URL path to exactly how the file was retrieved in cases where the source is a file; this provides an important bit of provenance in each record such that it can always be tracked back to its source
* file_name - Name advertised for the file
* file_date - Date advertised for the file from its web server host; this may or not be an accurate date last updated for the file; date/time strings are parsed and formatted to ISO8601
* file_size - When available, an advertised size for the file from its source repository or web server; these are variable strings with no attempt to standardize
* source_file_uploaded - An ISO-8601 date/time stamp for when the file was uploaded to its repository. This is available at least from ScienceBase for now and should be available from any future repositories used as sources.
* date_created - An ISO-8601 date/time stamp for when the individual record was built from its source
* collection_id - Standard text string found throughout the system with the unique collection identifier in which the record is found. Currently, these are the ScienceBase IDs for each collection.
* Collection Title - The title of the collection in which the individual record is found for context.
* Collection Link - A link to the collection record; currently a ScienceBase URL that can be followed to a collection landing page. This may shift or expand as we begin assigning IGSN identifiers and could also include one or more links to a collection owner's web system (from citation linkages in metadata).
* Collection Owner - List of collection owner names. This is very simplistic at this point and just an early attempt at bringing forward a little bit of additional contextual information. We will expand this from a list of strings to a data structure based on contact information in standard metadata.
* Collection Improvement Needed - String indicating issues discovered when trying to infuse collection-level information into an individual record. As we continue to examine what all information should be coming forward from collection records, infused into individual items, we will record additional details here to help understand how collections and their records can be improved.
* processing_errors - List of any specific processing errors that were discovered in the process of building a record such as invalid or missing geometry.

The majority of inbound records provided simple geospatial coordinates in the form of latitude and longitude separated with a comma. The processing logic handles a few corner cases discovered where this was handled differently such as separate fields in a table such that we end up with a consistent coordinates string. There is an outstanding issue where the majority of collections also do not provide a Coordinate Reference System, and for the time being, we made a choice to treat everything as WGS84, the standard CRS assumption when dealing with GeoJSON. Further examination will be needed to deal with the corner cases, including a few where the supplemental information field or some other part of the records indicate a different CRS.

Building on the standard coordinates string, the build_point_geometry() function attempts to build a valid GeoJSON point. This geometry, combined with the properties (both original and infused) to produce a valid GeoJSON feature, assembled into a list of dicts in the processing pipeline, and then packaged into a GeoJSON feature collection by the nggdpp_record_list_to_geojson() function. Feature collections are inserted into individual database collections (as feature documents) in MongoDB and then separate indexes in ElasticSearch. This results in "standardized" GeoJSON feature collections and features across the entire NDC with somewhat variable properties. Common elements from the properties can be pulled out, listed, and queried on in standard ways while allowing for variability to reflect what is in heterogeneous source data.

Spreadsheets
------------
Spreadsheet files are the predominant method of providing collection items to the NDC. A convention was established early in the project to provide a pipe-delimited text/CSV file using a mostly standardized set of field names. These files are quite problematic in terms of the highly variable ways that such files can be produced. The processing code has to account for this variability in terms of different text encodings, line terminators, sometimes different delimiters, and other dynamics. A pre-processing step is run with the inspect_response_content() function to examine and characterize the content of each file to help aid in subsequent processing logic.

As with all text files, the data properties are all flat in terms of each property being a simple string value of some kind. Subsequent processing steps will be built over time to evaluate original data properties and attempt to add value in various ways through synthesis and data fusion.

XML Files
---------
XML files conform to a very simple specification that was created at the start of the NGGDPP activity. They are essentially the same thing as the spreadsheet files except that they allow for a couple of multi-value properties containing links to online resources or browse graphics, which could provide for more robust digital representations of the material.

Standard Metadata Files
-----------------------
In a very few specialized cases, collections advertise a "Web Accessible Folder" (WAF) link where individual metadata files can be harvested representing the documentation of samples/collection artifacts. These were set up at a time when the State Geological Surveys were collaborating on the National Geothermal Data System, another funded activity to help build a catalog for discovery of geothermal data assets of various kinds. The architecture that informs this method is the US Geoscience Information Network, an effort that stressed standardized methods such as standards-compliant metadata and coordinated harvesting methods.

The two organizations that provided this method were the Arizona Geological Survey, providing several WAFs containing ISO19139 XML files, and the Alaska Division of Geological and Geophysical Surveys, providing a single WAF containing FGDC CSDGM records. In both of these cases, we took advantage of the `gis_metadata_parser <https://pypi.org/project/gis-metadata-parser/>`_ Python package to read the metadata and pull out a simple set of higher level properties in common across the standards. Further work will be done over time to more fully take advantage of the depth of information that may be available in some metadata records as we get into further data fusion processes.

Geometry from standard metadata will require further work over time. At this stage, we are pulling the bounding box, recording as part of properties, and then deriving a point in one of two ways. For bounding boxes containing full valid bounding coordinates, a polygon geometry is built and then a centroid taken to generate a representational point for simplicity of search and display. A convention was established in USGIN to take data that were originally represented and encode a bounding box into ISO metadata with the same east/west and north/south bounding coordinates. In those cases, we simply build a representative point coordinate string at one "corner" of the bounding box.

Web Services
------------

Processing Pipeline
===================


ScienceBase File Processor
--------------------------

Web Accessible Folder Processor
-------------------------------

Web Service Processor
---------------------

