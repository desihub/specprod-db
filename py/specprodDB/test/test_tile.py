# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.tile.
"""
# import os
import unittest
from unittest.mock import patch, call
from ..tile import get_options


class TestTile(unittest.TestCase):
    """Test specprodDB.tile
    """
    @classmethod
    def setUpClass(cls):
        """Create temporary directory.
        """
        # cls.testDir = mkdtemp()
        pass

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directory.
        """
        # if os.path.exists(cls.testDir):
        #     rmtree(cls.testDir)
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('sys.argv', ['load_specprod_tile', '12345'])
    def test_get_options(self):
        """Test parsing of command-line options.
        """
        options = get_options("This is a test.")
        self.assertFalse(options.public)
        self.assertFalse(options.verbose)
        self.assertFalse(options.overwrite)
        self.assertEqual(options.tile, 12345)
