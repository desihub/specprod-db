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

from astropy.table import Table

from desiutil.iers import freeze_iers
# from desiutil.names import radec_to_desiname
from desiutil.log import get_logger, DEBUG, INFO

from desispec.io.meta import findfile
from desispec.io.photo import gather_targetphot, gather_tractorphot

from . import load as db
from .util import no_sky


_fiberassign_cache = dict()


def fiberassign_file(tileid):
    """Find a fiberassign file associated with `tileid`.

    Parameters
    ----------
    tileid : :class:`int`
        The Tile ID.

    Returns
    -------
    :class:`str`
        The path to the fiberassign file corresponding to tileid.
    """
    global _fiberassign_cache
    if tileid not in _fiberassign_cache:
        f = findfile('fiberassignsvn', tile=tileid, readonly=True)
        _fiberassign_cache[tileid] = f
    return _fiberassign_cache[tileid]


def potential_targets(tileid):
    """Find potential targets associated with `tileid`.

    Sky targets are not returned.

    Parameters
    ----------
    tileid : :class:`int`
        The Tile ID.

    Returns
    -------
    :class:`~astropy.table.Table`
        A table containing potential target information.
    """
    potential_targets_table = Table.read(fiberassign_file(tileid), format='fits', hdu='TARGETS')
    no_sky_rows = no_sky(potential_targets_table)
    potential_targets_table = Table(potential_targets_table[no_sky_rows])
    return potential_targets_table


def potential_phototometry(tile, targets):
    """Assemble a Table of targets that will be used to find photometric data.

    `targets` is assumed to come from one tile that has not already been loaded.
    Any existing photometry already loaded will be excluded from the returned list.

    Parameters
    ----------
    tile : :class:`~specprodDB.load.Tile`
        The tile associated with `targets`.
    targets : :class:`~astropy.table.Table`
        Effectively a list of ``TARGETID``.

    Returns
    -------
    :class:`~astropy.table.Table`
        A Table that will be the input to photometric search functions.
    """
    potential_tractorphot_already_loaded = db.dbSession.query(db.Photometry.targetid).filter(db.Photometry.targetid.in_(targets['TARGETID'].tolist())).all()
    potential_tractorphot_not_already_loaded = np.ones((len(targets),), dtype=bool)
    if len(potential_tractorphot_already_loaded) > 0:
        db.log.info("Removing %d objects already loaded.", len(potential_tractorphot_already_loaded))
    for row in potential_tractorphot_already_loaded:
        potential_tractorphot_not_already_loaded[targets['TARGETID'] == row[0]] = False
    potential_cat = Table()
    potential_cat['TARGETID'] = targets['TARGETID'][potential_tractorphot_not_already_loaded]
    potential_cat['TILEID'] = tile.tileid
    potential_cat['TARGET_RA'] = targets['RA'][potential_tractorphot_not_already_loaded]
    potential_cat['TARGET_DEC'] = targets['DEC'][potential_tractorphot_not_already_loaded]
    # potential_cat['PETAL_LOC'] = targets['PETAL_LOC'][potential_tractorphot_not_already_loaded]
    potential_cat['SURVEY'] = tile.survey
    potential_cat['PROGRAM'] = tile.program
    return potential_cat


def targetphot(catalog):
    """Find the target data associated with the targets in `catalog`.

    Parameters
    ----------
    catalog : :class:`~astropy.table.Table`
        A list of objects.

    Returns
    -------
    :class:`~astropy.table.Table`
        A Table containing the targeting data.
    """
    potential_targetphot = gather_targetphot(catalog, racolumn='TARGET_RA', deccolumn='TARGET_DEC')
    potential_targetphot['SURVEY'] = catalog['SURVEY']
    potential_targetphot['PROGRAM'] = catalog['PROGRAM']
    potential_targetphot['TILEID'] = catalog['TILEID']
    inan = np.logical_or(np.isnan(potential_targetphot['PMRA']), np.isnan(potential_targetphot['PMDEC']))
    if np.any(inan):
        potential_targetphot['PMRA'][inan] = 0.0
        potential_targetphot['PMDEC'][inan] = 0.0
    return potential_targetphot


def tractorphot(catalog):
    """Find the photometry data associated with the targets in `catalog`.

    Parameters
    ----------
    catalog : :class:`~astropy.table.Table`
        A list of objects.

    Returns
    -------
    :class:`~astropy.table.Table`
        A Table containing the photometry data.
    """
    potential_tractorphot = gather_tractorphot(catalog, racolumn='TARGET_RA', deccolumn='TARGET_DEC')
    assert (np.where(potential_tractorphot['RELEASE'] == 0)[0] == np.where(potential_tractorphot['BRICKNAME'] == '')[0]).all()
    return potential_tractorphot


def load_tile_photometry(photometry):
    """Insert the data in `photometry` into the database.

    Parameters
    -------
    photometry : :class:`~astropy.table.Table`
        A Table containing the photometry data.
    """
    row_index = np.where(photometry['BRICKNAME'] != '')[0]
    load_photometry = db.Photometry.convert(photometry, row_index=row_index)
    if len(load_photometry) > 0:
        # db.dbSession.rollback()
        db.dbSession.add_all(load_photometry)
        db.dbSession.commit()
        db.log.info("Loaded %d rows of Photometry data.", len(load_photometry))
    else:
        db.log.info("No Photometry data to load.")
    return


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    # freeze_iers()
    #
    # command-line arguments
    #
    options = db.get_options("Load data for one tile into a specprod database.")
    #
    # Logging
    #
    log_level = DEBUG if options.verbose else INFO
    db.log = get_logger(log_level, timestamp=True)
    #
    # Cache specprod value.
    #
    try:
        specprod = os.environ['SPECPROD']
    except KeyError:
        db.log.critical("Environment variable SPECPROD is not defined!")
        return 1
    #
    # Read configuration file.
    #
    config = SafeConfigParser()
    r = config.read(options.config)
    if not (r and r[0] == options.config):
        db.log.critical("Failed to read configuration file: %s!", options.config)
        return 1
    if specprod not in config:
        db.log.critical("Configuration has no section for '%s'!", specprod)
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
    # target_summary = config[specprod].getboolean('target_summary')
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
