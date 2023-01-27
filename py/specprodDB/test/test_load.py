# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.load.
"""
import unittest
import os
from shutil import rmtree

try:
    import sqlalchemy
    sqlalchemy_available = True
except ImportError:
    sqlalchemy_available = False


class TestLoad(unittest.TestCase):
    """Test specprodDB.load
    """
    @classmethod
    def setUpClass(cls):
        """Create unique test filename in a subdirectory.
        """
        # from uuid import uuid1
        # cls.testfile = 'test-{uuid}/test-{uuid}.fits'.format(uuid=uuid1())
        # cls.testyfile = 'test-{uuid}/test-{uuid}.yaml'.format(uuid=uuid1())
        # cls.testbrfile = 'test-{uuid}/test-br-{uuid}.fits'.format(uuid=uuid1())
        cls.testDir = os.path.join(os.environ['HOME'], 'desi_test_database')
        cls.origEnv = {'SPECPROD': None,
                       "DESI_SPECTRO_DATA": None,
                       "DESI_SPECTRO_REDUX": None}
        cls.testEnv = {'SPECPROD': 'dailytest',
                       "DESI_SPECTRO_DATA": os.path.join(cls.testDir, 'spectro', 'data'),
                       "DESI_SPECTRO_REDUX": os.path.join(cls.testDir, 'spectro', 'redux')}
        for e in cls.origEnv:
            if e in os.environ:
                cls.origEnv[e] = os.environ[e]
            os.environ[e] = cls.testEnv[e]

    @classmethod
    def tearDownClass(cls):
        """Cleanup test files if they exist.
        """
        # for testfile in [cls.testfile, cls.testyfile, cls.testbrfile]:
        #     if os.path.exists(testfile):
        #         os.remove(testfile)
        #         testpath = os.path.normpath(os.path.dirname(testfile))
        #         if testpath != '.':
        #             os.removedirs(testpath)

        for e in cls.origEnv:
            if cls.origEnv[e] is None:
                del os.environ[e]
            else:
                os.environ[e] = cls.origEnv[e]

        if os.path.exists(cls.testDir):
            rmtree(cls.testDir)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @unittest.skipUnless(sqlalchemy_available, "SQLAlchemy not installed; skipping specprodDB class tests.")
    def test_datachallenge_classes(self):
        """Test SQLAlchemy classes in specprodDB.load.
        """
        from ..load import (Tile, Exposure, Frame, Fiberassign, Potential, Target)
        pass
