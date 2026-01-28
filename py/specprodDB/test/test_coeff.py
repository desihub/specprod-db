# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.coeff.
"""
# import os
import unittest
from unittest.mock import patch
import numpy as np
from astropy.table import Table
from ..coeff import get_options, copy_columns


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

    @patch('sys.argv', ['coeff_patch', '-v', 'zall-pix-iron.fits'])
    def test_get_options(self):
        """Test get_options().
        """
        options = get_options()
        self.assertTrue(options.verbose)
        self.assertEqual(options.zall, 'zall-pix-iron.fits')

    def test_copy_columns_zpix(self):
        """Test copy_columns() for a zpix table.
        """
        def rowfilter(catalog):
            return np.array([0, 1, 3])
        w = rowfilter('foo')
        catalog = Table()
        catalog['TARGETID'] = np.array([1, 2, 3, 4])
        catalog['SURVEY'] = np.array(['main']*4)
        catalog['PROGRAM'] = np.array(['dark']*4)
        catalog['HEALPIX'] = np.array([137]*4)
        catalog['DESINAME'] = np.array(['name']*4)
        # catalog['TILEID'] = np.array([314]*4)
        # catalog['SPGRPVAL'] = np.array([20250703]*4)
        # catalog['TARGET_RA'] = np.random.uniform(size=(4,))
        # catalog['TARGET_DEC'] = np.random.uniform(size=(4,))
        catalog['COEFF'] = np.random.normal(size=(4, 10))
        patch_table = copy_columns(catalog, rowfilter, 'pix')
        self.assertEqual(patch_table.meta['EXTNAME'], 'COEFF_PATCH')
        self.assertTrue((patch_table['TARGETID'] == np.array([1, 2, 4])).all())
        self.assertTrue((patch_table['COEFF_5'] == catalog['COEFF'][w, 5]).all())

    def test_copy_columns_ztile(self):
        """Test copy_columns() for a ztile table.
        """
        def rowfilter(catalog):
            return np.array([0, 1, 3])
        w = rowfilter('foo')
        catalog = Table()
        catalog['TARGETID'] = np.array([1, 2, 3, 4])
        # catalog['SURVEY'] = np.array(['main']*4)
        # catalog['PROGRAM'] = np.array(['dark']*4)
        # catalog['HEALPIX'] = np.array([137]*4)
        catalog['DESINAME'] = np.array(['name']*4)
        catalog['TILEID'] = np.array([314]*4)
        catalog['SPGRPVAL'] = np.array([20250703]*4)
        # catalog['TARGET_RA'] = np.random.uniform(size=(4,))
        # catalog['TARGET_DEC'] = np.random.uniform(size=(4,))
        catalog['COEFF'] = np.random.normal(size=(4, 10))
        patch_table = copy_columns(catalog, rowfilter, 'tilecumulative')
        self.assertEqual(patch_table.meta['EXTNAME'], 'COEFF_PATCH')
        self.assertTrue((patch_table['TARGETID'] == np.array([1, 2, 4])).all())
        self.assertTrue((patch_table['COEFF_5'] == catalog['COEFF'][w, 5]).all())

    def test_copy_columns_no_desiname(self):
        """Test copy_columns() for a zpix table without DESINAME.
        """
        def rowfilter(catalog):
            return np.array([0, 1, 3])
        w = rowfilter('foo')
        catalog = Table()
        catalog['TARGETID'] = np.array([1, 2, 3, 4])
        catalog['SURVEY'] = np.array(['main']*4)
        catalog['PROGRAM'] = np.array(['dark']*4)
        catalog['HEALPIX'] = np.array([137]*4)
        # catalog['DESINAME'] = np.array(['name']*4)
        # catalog['TILEID'] = np.array([314]*4)
        # catalog['SPGRPVAL'] = np.array([20250703]*4)
        catalog['TARGET_RA'] = np.random.uniform(size=(4,))
        catalog['TARGET_DEC'] = np.random.uniform(size=(4,))
        catalog['COEFF'] = np.random.normal(size=(4, 10))
        patch_table = copy_columns(catalog, rowfilter, 'pix')
        self.assertEqual(patch_table.meta['EXTNAME'], 'COEFF_PATCH')
        self.assertTrue((patch_table['TARGETID'] == np.array([1, 2, 4])).all())
        self.assertTrue((patch_table['COEFF_5'] == catalog['COEFF'][w, 5]).all())
