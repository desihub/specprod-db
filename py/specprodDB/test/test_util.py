# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.util.
"""
import unittest
from unittest.mock import patch, call
import importlib.resources as ir

from pytz import utc
import numpy as np

from ..util import (cameraid, frameid, surveyid, decode_surveyid, programid,
                    spgrpid, targetphotid, decode_targetphotid, zpixid, ztileid,
                    fiberassignid, convert_dateobs, checkgzip, no_sky, parse_pgpass)


class TestUtil(unittest.TestCase):
    """Test specprodDB.util.
    """
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_cameraid(self):
        """Test specprodDB.util.cameraid.
        """
        self.assertEqual(cameraid('b0'), 0)
        self.assertEqual(cameraid('r5'), 15)
        self.assertEqual(cameraid('z9'), 29)

    def test_frameid(self):
        """Test specprodDB.util.frameid.
        """
        self.assertEqual(frameid(12345, 'b0'), 1234500)
        self.assertEqual(frameid(54321, 'r5'), 5432115)
        self.assertEqual(frameid(9876543, 'z9'), 987654329)

    def test_surveyid(self):
        """Test specprodDB.util.surveyid.
        """
        self.assertEqual(surveyid('main'), 6)
        self.assertEqual(surveyid('special'), 2)
        with self.assertRaises(KeyError):
            p = surveyid('foo')

    def test_decode_surveyid(self):
        """Test specprodDB.util.decode_surveyid.
        """
        self.assertEqual(decode_surveyid(6), 'main')
        self.assertEqual(decode_surveyid(2), 'special')
        with self.assertRaises(KeyError):
            p = decode_surveyid(-1)

    def test_programid(self):
        """Test specprodDB.util.programid.
        """
        self.assertEqual(programid('bright'), 2)
        self.assertEqual(programid('dark'), 3)
        with self.assertRaises(KeyError):
            p = programid('foo')

    def test_spgrpid(self):
        """Test specprodDB.util.spgrpid.
        """
        self.assertEqual(spgrpid('cumulative'), 3)
        self.assertEqual(spgrpid('healpix'), 7)
        with self.assertRaises(KeyError):
            p = spgrpid('foo')

    def test_targetphotid(self):
        """Test specprodDB.util.targetphotid.
        """
        self.assertEqual(targetphotid(123456789, 1234, 'main'),
                         475368997848868212518973852949)

    def test_decode_targetphotid(self):
        """Test specprodDB.util.targetphotid.
        """
        self.assertEqual(decode_targetphotid(475368997848868212518973852949),
                         (123456789, 1234, 'main'))

    def test_zpixid(self):
        """Test specprodDB.util.zpixid.
        """
        self.assertEqual(zpixid(123456789, 'main', 'dark'),
                         237684487653473477223012617493)
        self.assertEqual(zpixid(123456789, 'main', 'bright'),
                         158456325139209139629468667157)

    def test_ztileid(self):
        """Test specprodDB.util.ztileid.
        """
        self.assertEqual(ztileid(123456789, 'cumulative', 20220613, 1234),
                         33503513911740049863153959922673896725)
        self.assertEqual(ztileid(123456789, 'pernight', 20220613, 4321),
                         65404985810578087757944284910788005141)

    def test_fiberassignid(self):
        """Test specprodDB.util.fiberassignid.
        """
        self.assertEqual(fiberassignid(123456789, 1234, 1234),
                         97767552565365474777390944865557)
        self.assertEqual(fiberassignid(123456789, 4321, 4321),
                         342344890303844583884202505391381)

    def test_convert_dateobs(self):
        """Test specprodDB.util.convert_dateobs.
        """
        ts = convert_dateobs('2019-01-03T01:11:33.247')
        self.assertEqual(ts.year, 2019)
        self.assertEqual(ts.month, 1)
        self.assertEqual(ts.microsecond, 247000)
        self.assertIsNone(ts.tzinfo)
        ts = convert_dateobs('2019-01-03T01:11:33.247', tzinfo=utc)
        self.assertEqual(ts.year, 2019)
        self.assertEqual(ts.month, 1)
        self.assertEqual(ts.microsecond, 247000)
        self.assertIs(ts.tzinfo, utc)

    @patch('specprodDB.util.exists')
    def test_checkgzip(self, mock_exists):
        """Basic test for existence.
        """
        mock_exists.return_value = True
        path = checkgzip('filename.txt')
        self.assertEqual(path, 'filename.txt')
        mock_exists.assert_called_once_with('filename.txt')

    @patch('specprodDB.util.exists')
    def test_checkgzip_gz(self, mock_exists):
        """Basic test for existence of gz file.
        """
        mock_exists.side_effect = [False, True]
        path = checkgzip('filename.txt')
        self.assertEqual(path, 'filename.txt.gz')
        mock_exists.assert_has_calls([call('filename.txt'), call('filename.txt.gz')])

    @patch('specprodDB.util.exists')
    def test_checkgzip_already_gz(self, mock_exists):
        """Basic test for existence of gz file that's uncompressed.
        """
        mock_exists.side_effect = [False, True]
        path = checkgzip('filename.txt.gz')
        self.assertEqual(path, 'filename.txt')
        mock_exists.assert_has_calls([call('filename.txt.gz'), call('filename.txt')])

    @patch('specprodDB.util.exists')
    def test_checkgzip_raises(self, mock_exists):
        """Basic test for existence of gz file; file does not exist.
        """
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError) as e:
            path = checkgzip('filename.txt')
        mock_exists.assert_has_calls([call('filename.txt'), call('filename.txt.gz')])
        self.assertEqual(str(e.exception), 'Neither filename.txt nor filename.txt.gz could be found!')

    def test_no_sky(self):
        """Test specprodDB.util.no_sky.
        """
        catalog = {'TARGETID': np.array([-123456789, 123456789, 1 << 59], dtype=np.int64)}
        self.assertListEqual(no_sky(catalog).tolist(), [1])

    @patch('specprodDB.util.expanduser')
    def test_parse_pgpass(self, mock_expand):
        """Test specprodDB.util.parse_pgpass.
        """
        mock_expand.return_value = str(ir.files('specprodDB.test') / 't' / "test.pgpass")
        self.assertEqual(parse_pgpass('server.example.com', 'user'),
                         'postgresql://user:password@server.example.com:5432/database')

    @patch('specprodDB.util.expanduser')
    def test_parse_pgpass_missing_file(self, mock_expand):
        """Test specprodDB.util.parse_pgpass with missing file.
        """
        mock_expand.return_value = '/no/such/file'
        self.assertIsNone(parse_pgpass('server.example.com', 'user'))

    @patch('specprodDB.util.expanduser')
    def test_parse_pgpass_missing_hostname(self, mock_expand):
        """Test specprodDB.util.parse_pgpass with missing hostname.
        """
        mock_expand.return_value = str(ir.files('specprodDB.test') / 't' / "test.pgpass")
        self.assertIsNone(parse_pgpass('none.example.com', 'user'))

    @patch('specprodDB.util.expanduser')
    def test_parse_pgpass_missing_username(self, mock_expand):
        """Test specprodDB.util.parse_pgpass with missing username.
        """
        mock_expand.return_value = str(ir.files('specprodDB.test') / 't' / "test.pgpass")
        self.assertIsNone(parse_pgpass('server.example.com', 'nobody'))
