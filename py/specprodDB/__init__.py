# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB
==========

Tools for loading DESI data into databases.

In particular, this code loads the spectroscopic production database.

It does *not* include:

#. Pipeline processing status database.
#. The full imaging and targeting databases, but only the imaging and
   targeting associated with a spectroscopic production.
"""
from ._version import __version__
