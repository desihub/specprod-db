# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.merge.
"""
# import os
import sys
import unittest
# from unittest.mock import patch, mock_open, call
from ..merge import get_options, detect_files, split_load


class TestMerge(unittest.TestCase):
    """Test specprodDB.merge.
    """
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
        cls.thirteen = sys.version_info.major == 3 and sys.version_info.minor >= 13

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_options(self):
        pass

    def test_detect_files(self):
        pass

    def test_split_load(self):
        pass
