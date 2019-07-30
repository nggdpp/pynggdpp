# pyNGGDPP PACKAGE

import pkg_resources  # part of setuptools


# Import bis objects
from . import aws
from . import sciencebase
from . import item_process
from . import rest_api

# provide version, PEP - three components ("major.minor.micro")
__version__ = pkg_resources.require("pynggdpp")[0].version


# metadata retrieval
def get_package_metadata():
    d = pkg_resources.get_distribution('pynggdpp')
    for i in d._get_metadata(d.PKG_INFO):
        print(i)
