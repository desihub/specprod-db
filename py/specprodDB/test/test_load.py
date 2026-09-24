# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.load.
"""
import os
import unittest
from unittest.mock import MagicMock, patch, call
from tempfile import mkdtemp
from shutil import rmtree
from ..load import load_file, setup_db, q3c_index, get_options, find_files, close_db


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
        self.assertEqual(options.datapath, '/global/cfs/cdirs/desi')
        self.assertFalse(options.verbose)
        self.assertFalse(options.overwrite)
        self.assertEqual(options.load, 'exposures')

    @patch('specprodDB.load.text')
    @patch('specprodDB.load.dbSession')
    @patch('specprodDB.load.get_logger')
    def test_q3c_index(self, mock_log, mock_session, mock_text):
        """Test creation of q3c index.
        """
        text = mock_text('CREATE INDEX IF NOT EXISTS ix_target_q3c_ang2ipix ON fuji.target (q3c_ang2ipix(tile_ra, tile_dec));\n    CLUSTER fuji.target USING ix_target_q3c_ang2ipix;\n    ANALYZE fuji.target;\n    ')
        q3c_index('fuji', 'target', ra='tile_ra')
        mock_session.execute.assert_called_once_with(text)
        mock_log().info.assert_has_calls([call("Creating q3c index on %s.%s.", 'fuji', 'target'),
                                          call("Finished q3c index on %s.%s.", 'fuji', 'target')])

    @patch('specprodDB.load.dbSession')
    @patch('specprodDB.load.engine')
    def test_close_db(self, mock_engine, mock_session):
        """Test actions that close db connection.
        """
        close_db()
        mock_session.close.assert_called()
        mock_engine.dispose.assert_called()

    @patch('glob.glob')
    @patch('os.path.exists')
    def test_find_files(self, mock_exists, mock_glob):
        """Test find_files.
        """
        specprod = 'mock_specprod'
        datapath = '/mock/desi'
        mock_exists.return_value = True
        globs = list()
        for t in ('photometry', 'target', 'ztile', 'zpix', 'fiberassign'):
            globs.append([os.path.join(datapath, f'{specprod}.{t}.{n:d}.fits')
                          for n in range(3)])
        mock_glob.side_effect = globs
        mock_options = MagicMock()
        mock_options.datapath = datapath
        mock_options.zcatalog = True
        config = {specprod: MagicMock()}
        config[specprod].getboolean.return_value = True
        d = {'release': 'dr2', 'photometry': 'v2.0', 'redshift': 'patch/v2'}
        config[specprod].__getitem__.side_effect = d.__getitem__
        filepaths = find_files(specprod, config, mock_options)
        self.assertEqual(filepaths['tile'],
                         '/mock/desi/spectro/redux/mock_specprod/tiles-mock_specprod.fits')
        self.assertEqual(filepaths['photometry'][0],
                         '/mock/desi/mock_specprod.photometry.0.fits')
