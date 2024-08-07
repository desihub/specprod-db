# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.tile
===============

Code for loading one or more separate tiles into the spectroscopic production database.

Notes
-----

Some of this code may be combined or otherwise refactored with :mod:`specprodDB.load`
in the future.
"""
import os
from configparser import SafeConfigParser

import numpy as np

from astropy import __version__ as astropy_version
from astropy.table import Table

from sqlalchemy import __version__ as sqlalchemy_version

from desiutil import __version__ as desiutil_version
from desiutil.iers import freeze_iers
# from desiutil.names import radec_to_desiname
from desiutil.log import get_logger, DEBUG, INFO

from . import __version__ as specprodDB_version
import specprodDB.load as db


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    global log
    freeze_iers()
    #
    # command-line arguments
    #
    options = db.get_options("Load data for one tile into a specprod database.")
    #
    # Logging
    #
    if options.verbose:
        log = get_logger(DEBUG, timestamp=True)
    else:
        log = get_logger(INFO, timestamp=True)
    db.log = log
    #
    # Cache specprod value.
    #
    try:
        specprod = os.environ['SPECPROD']
    except KeyError:
        log.critical("Environment variable SPECPROD is not defined!")
        return 1
    #
    # Read configuration file.
    #
    config = SafeConfigParser()
    r = config.read(options.config)
    if not (r and r[0] == options.config):
        log.critical("Failed to read configuration file: %s!", options.config)
        return 1
    if specprod not in config:
        log.critical("Configuration has no section for '%s'!", specprod)
        return 1
    #
    # Initialize DB
    #
    postgresql = db.setup_db(hostname=config[specprod]['hostname'],
                             username=config[specprod]['username'],
                             schema=options.schema,
                             overwrite=options.overwrite,
                             public=options.public,
                             verbose=options.verbose)
    #
    # Load configuration
    #
    release = config[specprod]['release']
    photometry_version = config[specprod]['photometry']
    target_summary = config[specprod].getboolean('target_summary')
    rsv = config[specprod]['redshift'].split('/')
    if len(rsv) == 2:
        redshift_type, redshift_version = rsv[0], rsv[1]
    else:
        redshift_type, redshift_version = rsv[0], 'v0'
    tiles_version = config[specprod]['tiles']
    #
    # Complete initialization
    #
    if options.overwrite:
        db.load_versions(photometry_version, f"{redshift_type}/{redshift_version}",
                         release, specprod, tiles_version)
    return 0
