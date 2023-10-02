# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.load.
"""
import os
import unittest
from unittest.mock import patch, call
from tempfile import mkdtemp
from shutil import rmtree
from ..load import load_file, setup_db, q3c_index, get_options


class TestLoad(unittest.TestCase):
    """Test specprodDB.load
    """
    @classmethod
    def setUpClass(cls):
        """Create temporary directory.
        """
        cls.testDir = mkdtemp()

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary directory.
        """
        if os.path.exists(cls.testDir):
            rmtree(cls.testDir)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('sys.argv', ['load_specprod_db', '/global/cfs/cdirs/desi'])
    def test_get_options(self):
        """Test parsing of command-line options.
        """
        options = get_options()
        self.assertEqual(options.dbfile, 'specprod.db')
        self.assertFalse(options.verbose)
        self.assertFalse(options.overwrite)
        self.assertEqual(options.load, 'exposures')

    @patch('specprodDB.load.dbSession')
    @patch('specprodDB.load.log')
    def test_q3c_index(self, mock_log, mock_session):
        """Test creation of q3c index.
        """
        with patch('specprodDB.load.schemaname', 'fuji'):
            q3c_index('target', ra='tile_ra')
        mock_session.execute.assert_called_once_with('CREATE INDEX IF NOT EXISTS ix_target_q3c_ang2ipix ON fuji.target (q3c_ang2ipix(tile_ra, tile_dec));\n    CLUSTER fuji.target USING ix_target_q3c_ang2ipix;\n    ANALYZE fuji.target;\n    ')
        mock_log.info.assert_has_calls([call("Creating q3c index on %s.%s.", 'fuji', 'target'),
                                        call("Finished q3c index on %s.%s.", 'fuji', 'target')])
