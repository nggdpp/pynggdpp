========
pynggdpp
========

This package provides all of the core functional logic associated with building and interfacing with the National Digital Catalog of Geological and Geophysical Data. An earlier instance of this package is in the GitHub repo for posterity, but it has been completely redesigned from what that was. I first ran into issues with the pyscaffold project I used to build out documentation being incompatible with the AWS Lambda environment where I set up a serverless approach to collection processing. That prompted this more barebones, scaled back version of the package. I still have a lot to learn about packaing Python code, so I apologize in advance for the messiness in this package and its modules and classes.

The intent of the package is to provide a master store of all the stuff needed to work the NDC from data processing to the functions that drive the REST API. The data processing pipeline that uses some of the functions in this package can be found at https://github.com/nggdpp/ndc-pipeline. All of the secure data system and AWS connections references in this package require environment variables to be set for whatever environment is being used. Everything was developed initially on local instances or Docker containers before being moved online, and the intent is for others to be able to jump in and hack on this code to make it better.

Install the Python package with PIP:

pip install git+https://github.com/nggdpp/pynggdpp.git

