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

from astropy.table import Table, join

from sqlalchemy.exc import IntegrityError

from desiutil.iers import freeze_iers
# from desiutil.names import radec_to_desiname
from desiutil.log import get_logger, DEBUG, INFO

from desispec.io.meta import findfile
from desispec.io.photo import gather_targetphot, gather_tractorphot
from desispec.scripts.zcatalog import read_redrock
from desispec.zcatalog import find_primary_spectra

from . import load as db
from .util import no_sky, common_options


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


def potential_photometry(tile, targets):
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
    inan = np.logical_or(np.isnan(potential_targetphot['PMRA']),
                         np.isnan(potential_targetphot['PMDEC']))
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


def load_photometry(photometry):
    """Insert the data in `photometry` into the database.

    Parameters
    ----------
    photometry : :class:`~astropy.table.Table`
        A Table containing the photometry data.

    Returns
    -------
    :class:`list`
        A list of :class:`~specprodDB.load.Photometry` objects loaded.
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
    return load_photometry


def load_targetphot(targetphot, loaded_photometry):
    """Load the photometry, such as it is, for objects that do not have Tractor
    photometry.

    Parameters
    ----------
    targetphot : :class:`~astropy.table.Table`
        A Table containing the targeting data.
    loaded_photometry : :class:`list`
        A list of :class:`~specprodDB.load.Photometry` objects already loaded.

    Returns
    -------
    :class:`list`
        A list of :class:`~specprodDB.load.Photometry` objects loaded.
    """
    #
    # Find TARGETID not already *just* loaded.
    #
    load_rows = np.zeros((len(targetphot),), dtype=bool)
    if len(loaded_photometry) > 0:
        loaded_targetid = Table()
        loaded_targetid['TARGETID'] = np.array([r.targetid for r in loaded_photometry])
        loaded_targetid['LS_ID'] = np.array([r.ls_id for r in loaded_photometry])
        j = join(targetphot['TARGETID', 'RELEASE'], loaded_targetid, join_type='left', keys='TARGETID')
        try:
            load_targetids = j['TARGETID'][j['LS_ID'].mask]
        except AttributeError:
            #
            # This means *every* TARGETID is already loaded.
            #
            pass
        else:
            unique_targetid, targetid_index = np.unique(targetphot['TARGETID'].data, return_index=True)
            for t in load_targetids:
                load_rows[targetid_index[unique_targetid == t]] = True
    #
    # Find TARGETID not loaded in previous cycles.
    #
    targetphot_already_loaded = db.dbSession.query(db.Photometry.targetid).filter(db.Photometry.targetid.in_(targetphot[load_rows]['TARGETID'].tolist())).all()
    targetphot_not_already_loaded = np.ones((len(targetphot),), dtype=bool)
    for row in targetphot_already_loaded:
        targetphot_not_already_loaded[targetphot['TARGETID'] == row[0]] = False

    row_index = np.where(load_rows & targetphot_not_already_loaded)[0]
    load_targetphot = db.Photometry.convert(targetphot, row_index=row_index)
    if len(load_targetphot) > 0:
        # db.dbSession.rollback()
        db.dbSession.add_all(load_targetphot)
        db.dbSession.commit()
        db.log.info("Loaded %d rows of Photometry data (from targeting).", len(load_targetphot))
    else:
        db.log.info("No Photometry data (from targeting) to load.")
    return load_targetphot


def load_target(tile, target):
    """Load the targeting data associated with `tile`.

    Parameters
    ----------
    tile : :class:`~specprodDB.load.Tile`
        The tile associated with `target`.
    target : :class:`~astropy.table.Table`
        Effectively a list of ``TARGETID``.

    Returns
    -------
    :class:`list`
        A list of :class:`~specprodDB.load.Target` objects loaded.
    """
    load_target = db.Target.convert(target, tile.survey, tile.tileid)
    if len(load_target) > 0:
        # db.dbSession.rollback()
        db.dbSession.add_all(load_target)
        db.dbSession.commit()
        db.log.info("Loaded %d rows of Target data.", len(load_target))
    else:
        db.log.info("No Target data to load.")
    return load_target


def load_redshift(tile, spgrp='cumulative'):
    """Load redshift data associated with `tile`.

    Parameters
    ----------
    tile : :class:`~specprodDB.load.Tile`
        The tile with redshifts to load.
    spgrp : :class:`str`, optional
        The type of redshift data to load. Currently only 'cumulative' is supported.

    Returns
    -------
    :class:`list`
        A list of :class:`~specprodDB.load.Ztile` objects loaded.

    Raises
    ------
    ValueError
        If the value of `spgrp` is not supported.
    """
    if spgrp not in ('cumulative',):
        msg = 'Unsupported spgrp value: "%s"!'
        db.log.critical(msg, spgrp)
        raise ValueError(msg % (spgrp,))
    redrock_files = list()
    for spectrograph in range(10):
        redrock_file, redrock_exists = findfile('redrock_tile', groupname=spgrp,
                                                tile=tile.tileid, spectrograph=spectrograph,
                                                night=tile.lastnight,
                                                readonly=True, return_exists=True)
        if redrock_exists:
            redrock_files.append(redrock_file)
        else:
            zbest_file = redrock_file.replace('redrock', 'zbest')
            if os.path.exists(zbest_file):
                db.log.info('Using %s instead of %s.',
                            os.path.basename(zbest_file),
                            os.path.basename(redrock_file))
                redrock_files.append(zbest_file)
    if len(redrock_files) == 0:
        db.log.warning("No %s redrock or zbest files found for tile %d!", spgrp, tile.tileid)
        return []
    load_ztile = list()
    for rr in redrock_files:
        redrock_table, expfibermap = read_redrock(rr, group=spgrp,
                                                  recoadd_fibermap=True,
                                                  pertile=True)
        assert (expfibermap['TILEID'] == tile.tileid).all()
        #
        # In non-daily specprod, firstnight is a minimum over all petals.
        # However here, we are doing a minimum over one petal.
        # Compare the FIRSTNIGHT calculation in desispec.scripts.zcatalog.
        #
        firstnight = np.min(expfibermap['NIGHT']).tolist()
        row_index = no_sky(redrock_table)
        load_ztile += db.Ztile.convert(redrock_table, tile.survey, tile.program,
                                       tile.tileid, firstnight,
                                       row_index=row_index)
    if len(load_ztile) > 0:
        statement = db.upsert(load_ztile)
        db.dbSession.execute(statement)
        # db.dbSession.add_all(load_ztile)
        db.dbSession.commit()
        db.log.info("Loaded %d rows of Ztile data.", len(load_ztile))
    else:
        db.log.info("No Ztile data to load.")
    return load_ztile


def load_fiberassign(tile):
    """Load the fiber assignments and potential assignments for `tile`.

    Parameters
    ----------
    tile : :class:`~specprodDB.load.Tile`
        The tile with fiber assignments to load.

    Returns
    -------
    :class:`tuple`
        A tuple containing the lists of :class:`~specprodDB.load.Fiberassign`
        and :class:`~specprodDB.load.Potential` objects loaded.
    """
    fiberassign_table = Table.read(fiberassign_file(tile.tileid), format='fits', hdu='FIBERASSIGN')
    potential_table = Table.read(fiberassign_file(tile.tileid), format='fits', hdu='POTENTIAL_ASSIGNMENTS')
    row_index = no_sky(fiberassign_table)
    load_fiberassign = db.Fiberassign.convert(fiberassign_table, tile.tileid, row_index=row_index)
    if len(load_fiberassign) > 0:
        db.dbSession.add_all(load_fiberassign)
        db.dbSession.commit()
        db.log.info("Loaded %d rows of Fiberassign data.", len(load_fiberassign))
    else:
        db.log.info("No Fiberassign data to load.")
    row_index = no_sky(potential_table)
    load_potential = db.Potential.convert(potential_table, tile.tileid, row_index=row_index)
    if len(load_potential) > 0:
        db.dbSession.add_all(load_potential)
        db.dbSession.commit()
        db.log.info("Loaded %d rows of Potential data.", len(load_potential))
    else:
        db.log.info("No Potential data to load.")
    return (load_fiberassign, load_potential)


def update_primary():
    """Update the primary classification after some number of tiles has been loaded.
    """
    zall_tilecumulative = db.dbSession.query(db.Ztile).all()
    zall_table = Table(list(zip(*[(z.targetid, z.zwarn, z.tsnr2_lrg) for z in zall_tilecumulative])),
                       names=('TARGETID', 'ZWARN', 'TSNR2_LRG'))
    nspec, primary = find_primary_spectra(zall_table)
    zcat_nspec, zcat_primary = nspec.tolist(), primary.tolist()
    for k, z in enumerate(zall_tilecumulative):
        z.zcat_nspec = zcat_nspec[k]
        z.zcat_primary = zcat_primary[k]
    db.dbSession.commit()
    db.log.info("Updated primary classification for %d Ztile objects.", len(zall_tilecumulative))
    sv_tilecumulative = db.dbSession.query(db.Ztile).filter(db.Ztile.survey.in_(('sv1', 'sv2', 'sv3'))).all()
    if len(sv_tilecumulative) > 0:
        sv_table = Table(list(zip(*[(z.targetid, z.zwarn, z.tsnr2_lrg) for z in sv_tilecumulative])),
                         names=('TARGETID', 'ZWARN', 'TSNR2_LRG'))
        nspec, primary = find_primary_spectra(sv_table)
        sv_nspec, sv_primary = nspec.tolist(), primary.tolist()
        for k, z in enumerate(sv_tilecumulative):
            z.sv_nspec = sv_nspec[k]
            z.sv_primary = sv_primary[k]
        db.dbSession.commit()
        db.log.info("Updated primary classification for %d SV Ztile objects.", len(sv_tilecumulative))
    else:
        db.log.info("No SV Ztile objects to update.")
    main_tilecumulative = db.dbSession.query(db.Ztile).filter(db.Ztile.survey.in_(('main', ))).all()
    if len(main_tilecumulative) > 0:
        main_table = Table(list(zip(*[(z.targetid, z.zwarn, z.tsnr2_lrg) for z in main_tilecumulative])), names=('TARGETID', 'ZWARN', 'TSNR2_LRG'))
        nspec, primary = find_primary_spectra(main_table)
        main_nspec, main_primary = nspec.tolist(), primary.tolist()
        for k, z in enumerate(main_tilecumulative):
            z.main_nspec = main_nspec[k]
            z.main_primary = main_primary[k]
        db.dbSession.commit()
        db.log.info("Updated primary classification for %d Main Ztile objects.", len(main_tilecumulative))
    else:
        db.log.info("No Main Ztile objects to update.")
    return


def update_q3c():
    """Update the q3c indexes after some number of tiles has been loaded.
    """
    q3c_updates = {'tile': 'tilera', 'exposure': 'tilera',
                   'photometry': 'ra', 'fiberassign': 'target_ra'}
    for table in q3c_updates:
        db.q3c_index(table, ra=q3c_updates[table])
    return


def get_options(description="Load data for one tile into a specprod database."):
    """Parse command-line options.

    Parameters
    ----------
    description : :class:`str`, optional
        Override the description in the command-line help.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed options.
    """
    prsr = common_options(description)
    prsr.add_argument('-e', '--exposures-file', action='store', dest='exposures_file', metavar='FILE',
                      help='Override the top-level exposures file associated with a specprod.')
    prsr.add_argument('-p', '--primary', action='store_true', dest='primary',
                      help='Update primary redshift values and indexes for all tiles.')
    prsr.add_argument('-t', '--tiles-file', action='store', dest='tiles_file', metavar='FILE',
                      help='Override the top-level tiles file associated with a specprod.')
    prsr.add_argument('-u', '--update', action='store_true', dest='update',
                      help='Specify that this is an update to an already-loaded tile.')
    prsr.add_argument('tile', metavar='TILEID', type=int, help='Load TILEID.')
    options = prsr.parse_args()
    return options


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    #
    # command-line arguments.
    #
    options = get_options("Load data for one tile into a specprod database.")
    #
    # Logging.
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
    # Initialize DB.
    #
    freeze_iers()
    postgresql = db.setup_db(hostname=config[specprod]['hostname'],
                             username=config[specprod]['username'],
                             schema=options.schema,
                             overwrite=options.overwrite,
                             public=options.public,
                             verbose=options.verbose)
    #
    # Load configuration.
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
    # Complete initialization.
    #
    if options.overwrite:
        db.load_versions(photometry_version, f"{redshift_type}/{redshift_version}",
                         release, specprod, tiles_version)
    #
    # Find the tile in the top-level tiles file.
    #
    if options.tiles_file is None:
        tiles_file = findfile('tiles', readonly=True)
    else:
        tiles_file = options.tiles_file
    tiles_table = Table.read(tiles_file, format='fits', hdu='TILES')
    row_index = np.where(tiles_table['TILEID'] == options.tile)[0]
    if len(row_index) == 1:
        candidate_tiles = db.Tile.convert(tiles_table, row_index=row_index)
    elif len(row_index) > 1:
        db.log.critical("Multiple matching tiles found in %s for tile %d!",
                        tiles_file, options.tile)
        return 1
    else:
        db.log.critical("No matching tiles found in %s for tile %d!",
                        tiles_file, options.tile)
        return 1
    #
    # Find the associated exposures.
    #
    if options.exposures_file is None:
        exposures_file = findfile('exposures', readonly=True)
    else:
        exposures_file = options.exposures_file
    exposures_table = Table.read(exposures_file, format='fits', hdu='EXPOSURES')
    row_index = np.where((exposures_table['TILEID'] == candidate_tiles[0].tileid) & (exposures_table['EFFTIME_SPEC'] > 0))[0]
    if len(row_index) > 0:
        load_exposures = db.Exposure.convert(exposures_table, row_index=row_index)
    else:
        db.log.critical("No valid exposures found for tile %d, even though EFFTIME_SPEC == %f!",
                        candidate_tiles[0].tileid, candidate_tiles[0].efftime_spec)
        return 1
    frames_table = Table.read(exposures_file, format='fits', hdu='FRAMES')
    load_frames = list()
    for exposure in load_exposures:
        row_index = np.where(frames_table['EXPID'] == exposure.expid)[0]
        assert len(row_index) > 0
        load_frames += db.Frame.convert(frames_table, row_index=row_index)
    try:
        statement = db.upsert(candidate_tiles)
        db.dbSession.execute(statement)
        # db.dbSession.add_all(candidate_tiles)
        db.dbSession.commit()
    except IntegrityError as exc:
        #
        # IntegrityError is thrown when a tile is already loaded, but also when
        # a NOT NULL constraint is violated.
        #
        db.log.critical("Tile %d cannot be loaded!", candidate_tiles[0].tileid)
        db.log.critical("Message was: %s", exc.args[0])
        db.dbSession.rollback()
        return 1
    new_tile = candidate_tiles[0]
    try:
        statement = db.upsert(load_exposures)
        db.dbSession.execute(statement)
        # db.dbSession.add_all(load_exposures)
        db.dbSession.commit()
    except IntegrityError as exc:
        db.log.critical("Exposures for tile %d cannot be loaded!", new_tile.tileid)
        db.log.critical("Message was: %s", exc.args[0])
        db.dbSession.rollback()
        db.dbSession.delete(new_tile)
        db.dbSession.commit()
        return 1
    statement = db.upsert(load_frames)
    db.dbSession.execute(statement)
    # db.dbSession.add_all(load_frames)
    db.dbSession.commit()
    #
    # Load photometry. If this is an update, these should already be loaded.
    #
    if not options.update:
        potential_targets_table = potential_targets(new_tile.tileid)
        potential_cat = potential_photometry(new_tile, potential_targets_table)
        potential_targetphot = targetphot(potential_cat)
        potential_tractorphot = tractorphot(potential_cat)
        loaded_photometry = load_photometry(potential_tractorphot)
        loaded_targetphot = load_targetphot(potential_targetphot, loaded_photometry)
        #
        # Load targeting table.
        #
        loaded_target = load_target(new_tile, potential_targetphot)
        #
        # Load fiberassign and potential.
        #
        loaded_fiberassign, loaded_potential = load_fiberassign(new_tile)
    #
    # Load tile/cumulative redshifts.
    #
    loaded_ztile = load_redshift(new_tile)
    #
    # Update global values, if requested.
    #
    if options.primary:
        update_primary()
        update_q3c()
    #
    # Clean up.
    #
    db.dbSession.close()
    db.engine.dispose()
    return 0
