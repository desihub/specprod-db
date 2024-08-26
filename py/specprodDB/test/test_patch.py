# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.patch.
"""
import os
import unittest
from unittest.mock import patch, mock_open, call
from ..batch import get_options, get_data
# from .. import __version__ as specprod_db_version


class TestPatch(unittest.TestCase):
    """Test specprodDB.patch.
    """
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('sys.argv', ['patch_specprod', '-v', '.'])
    def test_get_options(self):
        """Test get_options().
        """
        options = get_options()
        self.assertTrue(options.verbose)
        self.assertEqual(options.src, 'jura')
        self.assertEqual(options.dst, 'daily')
        self.assertEqual(options.output, '.')
        self.assertFalse(options.overwrite)
