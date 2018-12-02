============================
National Digital Catalog 2.0
============================

The National Digital Catalog (NDC) is a foundational resource provided for the `National Geological and Geophysical Data Preservation Program <http://datapreservation.usgs.gov>`_ (NGGDPP), a government program conducted by the `U.S. Geological Survey <https://www.usgs.gov>`_ in partnership with the `Association of American State Geologists <http://www.stategeologists.org/>`_. The goal of the NDC is to provide a comprehensive point of discovery and exploration for the Nation's treasure trove of scientific samples from rock cores to aerial photographs and well logs. These assets are invaluable scientific resources in continued exploration for natural resources, pursuit of climate change adaptation strategies, and overall understanding of the complex earth system.

This documentation describes the transition to a new architecture with details about the pyNGGDPP package, the online infrastructure that package interacts with, and the changes put into place since the first generation of the NDC. The software components in this project serve the needs of the data assembly process to build the catalog. They are provided in the public domain in order to promote full transparency and traceability in the process and to serve as building blocks for development of tools built on the NDC infrastructure.

Documentation is organized into a series of articles that describe aspects of the NDC and its history along with technical details on associated software, metadata, and data components.

Collection Metadata Strategy
============================
The original records for NDC collections came from a survey sent out to State Geological Surveys at the start of the NGGDPP. The first year of the program asked State Surveys and U.S. Department of the Interior Bureaus to conduct a high level inventory of their collections and provide basic information describing the collections in need of preservation actions. Survey responses served as the start to collection metadata records, and those responses are still housed as "metadata.xml" files attached to many of the collection items in ScienceBase. Read more about the current strategy for `collectionmetadata`_.

ScienceBase
===========
The original version of the National Digital Catalog was built in `USGS ScienceBase <https://www.sciencebase.gov>`_, a cataloging and digital repository system. It is still the core of the NDC as a catalog, but the method for indexing items from the collections into an overall search index, API, and set of services has shifted to different technology. Read more about how :ref:`sciencebase`_ works in the new architecture.

International GeoSample Number
==============================
The International GeoSample Number (IGSN) is now the globally recognized mechanism for registering, identifying, and cataloging physical scientific assets in the geosciences in a way that they can be referenced in publications and other venues for traceability and discovery. The USGS is now an allocating agent for IGSN identifiers, and the infrastructure for supporting this functionality is being built into the NDC. Read about the approach for :ref:`igsn`_ implementation.

Sample/Artifact Index
=====================
The overall goal of the NDC has been to provide as much information as possible through a single index or access point about the actual physical samples and other artifacts within collections. The first generation of the NDC accomplished this through some add-on tools in ScienceBase that allowed files containing sample records to be harvested into individual ScienceBase Items within collections. This made them available through the ScienceBase Catalog search tool (also available as an API) and through geospatial web services that allowed sample points to be mapped. The new generation of the NDC builds sample collections into a new type of more flexible and extensible search index while maintaining the collections in ScienceBase. Read more about the :ref:`collectionindex`_.

Collection Harvesting Methods
=============================
The first generation NDC revolved almost entirely around the process of grantees under the NGGDPP generating and uploading sample inventories in a very simple data structure as one of two types of files (XML or CSV). This had the effect of establishing a very simple process to follow that allowed the search index to grow to over 3 million individual items. However, the simplistic approach also resulted in far less information being available for search and discovery than is actually available behind collection inventories, a more difficult secondary process for some collection owners to go through, and a number of other technical challenges. The new strategy works toward investing in capacity development across the network of collection owners and an ability to advertise and take advantage of new and more robust routes of accessing collection information. Reach more about :ref:`harvesting`_.

Application Programming Interface
=================================
One of the goals of the first generation NDC was to provide State Surveys and other groups a set of online capabilities managed centrally on sustainable government infrastructure that could be leveraged for many different applications within a local or thematic context. The NDC was built within USGS ScienceBase to provide the sustainable and supported infrastructure and an open Application Programming Interface (API) that anyone could build on. ScienceBase also supported Open Geospatial Consortium web services that could be used through standard online and desktop mapping tools. However, not many of these types of applications were ever realized, possibly due to the relative simplicity of the information model used in the first generation and subsequent lack of important and synthesized details to work with. Read more about the strategy to improve access for programmers in the new :ref:`api`_.

Linked Data
===========
Starting with controlled pick lists in the original collection survey, the first generation NDC attempted to guide the process toward community definitions of key concepts in the information model such as sample types, rock types, geologic formations, and other important details needed to enhance discovery and use of the system. However, the overall open process used in accepting information from very heterogeneous underlying information systems and processes has meant that the resulting national catalog lacks any real semblance of controlled vocabularies for important concepts that could be used to find information in common across collections. Much has happened in the broader world of geoinformatics in the last decade that will enable the NDC of the future to become much smarter and more capable in providing value-added functionality on top of what will remain a heterogeneous and flexible system of loosely coupled information systems. Read more about :ref:`linkeddata`_ strategies in the new NDC.

pyNGGDPP Package
================
Software and coding methods are an integral part of the new generation NDC. The architecture is being designed to be a completely code-driven system from the point that a new collection comes into the catalog or an existing collection is updated. Software will constantly monitor the catalog for changes and begin taking immediate action to incorporate new information, evaluate harvestable routes to collection records, and build new value into the system. The core processing logic is being built in the Python language and is incorporated into a version controlled package that can be used by anyone with an interest in building on what we've started. Read more about the details of :ref:`pynggdpp`_ and learn how you can contribute.

Contents
========

.. toctree::
   :maxdepth: 2

   collectionmetadata
   sciencebase
   igsn
   collectionindex
   harvesting
   api
   linkeddata
   pynggdpp
   license
   authors
   changelog


