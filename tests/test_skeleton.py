#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pynggdpp.skeleton import fib

__author__ = "sbristol@usgs.gov"
__copyright__ = "sbristol@usgs.gov"
__license__ = "public-domain"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
