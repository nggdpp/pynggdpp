#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pynggdpp import collection

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "public-domain"


def test_ndc_collection_type_tag():
    assert type(collection.ndc_collection_type_tag('ndc_collection')) is dict
    assert type(collection.ndc_collection_type_tag('blah blah')) is None
    with pytest.raises(AssertionError):
        pytest.fail('Returned an invalid response')
