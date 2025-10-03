# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.coeff.
"""
import os
import unittest
from unittest.mock import patch, mock_open, call
from ..coeff import get_options

class TestCoeff(unittest.TestCase):
    """Test specprodDB.coeff.
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

    @patch('sys.argv', ['patch_coeff', '-v', 'zall-pix-iron.fits'])
    def test_get_options(self):
        """Test get_options().
        """
        options = get_options()
        self.assertTrue(options.verbose)
        self.assertEqual(options.zall, 'zall-pix-iron.fits')
