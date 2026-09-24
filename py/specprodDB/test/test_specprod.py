# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test importing of specprod-specific modules.
"""
# import os
# import sys
import unittest
from importlib import import_module
# from unittest.mock import patch, mock_open, call


class TestSpecprod(unittest.TestCase):
    """Test importing of specprod-specific modules.
    """

    def test_import_specprod(self):
        """
        """
        for schema in ('daily', 'fuji', 'guadalupe', 'iron', 'loa',
                       'matterhorn', 'nevis'):
            if schema == 'nevis':
                with self.assertRaises(ImportError):
                    schemamodule = import_module(f"specprodDB.{schema}")
            else:
                schemamodule = import_module(f"specprodDB.{schema}")
                for table in ('Version', 'Photometry', 'Target', 'Tile', 'Exposure',
                              'Frame', 'Fiberassign', 'Potential', 'Ztile'):
                    self.assertTrue(hasattr(schemamodule, table))
                if schema != 'daily':
                    self.assertTrue(hasattr(schemamodule, 'Zpix'))
