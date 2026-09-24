# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.base.
"""
import os
import sys
import unittest
from unittest.mock import patch, mock_open, call
from sqlalchemy.orm import (DeclarativeBase, declarative_mixin, declared_attr)
from ..base import Base, schema_mixin_factory


class TestBase(unittest.TestCase):
    """Test specprodDB.base.
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

    def test_base(self):
        """Test Base class.
        """
        self.assertTrue(issubclass(Base, DeclarativeBase))

    def test_schema_mixin_factory(self):
        """Test the mixin generator.
        """
        mixin_class = schema_mixin_factory('mock_schema')
        self.assertTrue(hasattr(mixin_class, '__tablename__'))
        self.assertTrue(hasattr(mixin_class, '__table_args__'))
