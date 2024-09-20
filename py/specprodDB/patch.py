# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.patch
================

Patch top-level exposures and tiles summary files. This is meant to be applied
to the ``daily`` specprod, but others could be patched, in principle.
"""
import os
import datetime
import sys
from shutil import copy2
from argparse import ArgumentParser
import numpy as np
import pytz
from astropy.table import Table, join
from astropy.io import fits
from desiutil.log import get_logger, DEBUG, INFO
# from desispec.io import read_table
from desispec.io.meta import faflavor2program
from specprodDB.util import cameraid


def match_rows(left, right):
    """Match rows in `left` to rows in `right`.

    Parameters
    ----------
    src : array-like
        The column to be matched. This could be "artificial".
    dst : array-like
        The column to be matched. This could be "artificial".

    Returns
    -------
    :class:`tuple`
        The row indexes of `left` and `right` that match.
    """
    left_join = Table()
    left_join['JOIN_ID'] = left
    left_join['LEFT_INDEX'] = np.arange(len(left))
    right_join = Table()
    right_join['JOIN_ID'] = right
    right_join['RIGHT_INDEX'] = np.arange(len(right))
    joined = join(left_join, right_join, join_type='outer', keys='JOIN_ID')
    if hasattr(joined['LEFT_INDEX'], 'mask'):
        good_join = (~joined['LEFT_INDEX'].mask)
    else:
        good_join = (joined['LEFT_INDEX'] >= 0)
    if hasattr(joined['RIGHT_INDEX'], 'mask'):
        good_join = good_join & (~joined['RIGHT_INDEX'].mask)
    return (joined['LEFT_INDEX'][good_join], joined['RIGHT_INDEX'][good_join])


def zero_fill(data, label):
    """Fill any masked values in `data` with zero.

    Parameters
    ----------
    data : :class:`~astropy.table.Table`
        A data table.
    label : :class:`str`
        A label to use in logging.

    Returns
    -------
    :class:`~astropy.table.Table`
        The modified `data` table.
    """
    log = get_logger()
    for column in data.colnames:
        if hasattr(data[column], 'mask'):
            if data[column].mask.any():
                log.info("Replacing %d masked values in dst_%s column %s with zero.",
                         np.sum(data[column].mask), label, column)
                data[column][data[column].mask] = 0
                data[column].mask[data[column].mask] = False
    return data


def patch_frames(src_frames, dst_frames):
    """Patch frames data in `dst_frames` with the data in `src_frames`.

    Parameters
    ----------
    src_frames : :class:`~astropy.table.Table`
        Source of frames data.
    dst_frames : :class:`~astropy.table.Table`
        Data to be patched.

    Returns
    -------
    :class:`~astropy.table.Table`
        A *copy* of `dst_frames` with data replaced from `src_frames`.
    """
    log = get_logger()
    src_frames_index, dst_frames_index = match_rows(np.array([100*row['EXPID'] + cameraid(row['CAMERA']) for row in src_frames]),
                                                    np.array([100*row['EXPID'] + cameraid(row['CAMERA']) for row in dst_frames]))
    dst_frames_patched = dst_frames.copy()
    for column in dst_frames_patched.colnames:
        if (column in src_frames.colnames and hasattr(src_frames[column], 'mask') and np.any(src_frames[column].mask[src_frames_index])):
            #
            # For simplicity, the code below replaces all masked values,
            # but further cuts will restrict to the rows we care about.
            #
            log.info("Replacing masked values in src_frames column %s with zero.", column)
            src_frames[column][src_frames[column].mask] = 0
            src_frames[column].mask[src_frames[column].mask] = False
        if hasattr(dst_frames_patched[column], 'mask') and column != 'TSNR2_ALPHA':
            if np.any(dst_frames_patched[column].mask[dst_frames_index]):
                log.info("Patching %d rows in dst_frames column %s.",
                         np.sum(dst_frames_patched[column].mask[dst_frames_index]), column)
                src_frames_matched = src_frames[column][src_frames_index]
                dst_frames_matched = dst_frames_patched[column][dst_frames_index]
                dst_frames_mask_matched = dst_frames_patched[column].mask[dst_frames_index]
                assert np.sum(dst_frames_mask_matched) == np.sum(dst_frames_patched[column].mask[dst_frames_index])
                dst_frames_matched[dst_frames_mask_matched] = src_frames_matched[dst_frames_mask_matched]
                dst_frames_matched.mask[dst_frames_mask_matched] = False
                dst_frames_patched[column][dst_frames_index] = dst_frames_matched
                dst_frames_patched[column].mask[dst_frames_index] = dst_frames_matched.mask
                #
                # Some values should have changed!
                #
                assert not (dst_frames_patched[column].data.data == dst_frames[column].data.data).all()
    dst_frames_patched = zero_fill(dst_frames_patched, 'frames')
    return dst_frames_patched


def patch_exposures(src_exposures, dst_exposures, first_night=None):
    """Patch exposures data in `dst_exposures` with the data in `src_exposures`.

    Parameters
    ----------
    src_exposures : :class:`~astropy.table.Table`
        Source of tiles data.
    dst_exposures : :class:`~astropy.table.Table`
        Data to be patched.
    first_night : :class:`int`, optional
        The earliest night of data that will ever be loaded. If not set, it
        will be derived from the first exposure in `src_exposures`

    Returns
    -------
    :class:`~astropy.table.Table`
        A *copy* of `dst_exposures` with data replaced from `src_exposures`.
    """
    log = get_logger()
    if first_night is None:
        first_src_exposure = src_exposures['EXPID'].min()
        first_night = src_exposures['NIGHT'][src_exposures['EXPID'] == first_src_exposure].min()
    #
    # Set up a join.
    #
    src_exposures_index, dst_exposures_index = match_rows(src_exposures['EXPID'], dst_exposures['EXPID'])
    dst_exposures_bad_coord = ((dst_exposures['TILERA'][dst_exposures_index] == 0) &
                               (dst_exposures['TILEDEC'][dst_exposures_index] == 0))
    #
    # Apply patches from src_exposures.
    #
    dst_exposures_patched = dst_exposures.copy()
    can_patch = ('NIGHT', 'EXPID', 'TILEID', 'TILERA', 'TILEDEC', 'MJD',
                 'SURVEY', 'PROGRAM', 'FAPRGRM', 'FAFLAVOR', 'EXPTIME',
                 'GOALTIME', 'GOALTYPE', 'MINTFRAC', 'AIRMASS', 'EBV',
                 'SEEING_ETC', 'EFFTIME_ETC', 'TRANSPARENCY_GFA', 'SEEING_GFA',
                 'FIBER_FRACFLUX_GFA', 'FIBER_FRACFLUX_ELG_GFA', 'FIBER_FRACFLUX_BGS_GFA',
                 'FIBERFAC_GFA', 'FIBERFAC_ELG_GFA', 'FIBERFAC_BGS_GFA', 'AIRMASS_GFA',
                 'SKY_MAG_AB_GFA', 'EFFTIME_GFA', 'EFFTIME_DARK_GFA',
                 'EFFTIME_BRIGHT_GFA', 'EFFTIME_BACKUP_GFA')
    for column in ['TILERA', 'TILEDEC', 'MJD', 'SURVEY'] + [c for c in dst_exposures_patched.colnames
                                                            if hasattr(dst_exposures_patched[c], 'mask') and c in can_patch]:
        if (column in src_exposures.colnames and hasattr(src_exposures[column], 'mask') and np.any(src_exposures[column].mask[src_exposures_index])):
            #
            # For simplicity, the code below replaces all masked values,
            # but further cuts will restrict to the rows we care about.
            #
            log.info("Replacing masked values in src_exposures column %s with zero.", column)
            src_exposures[column][src_exposures[column].mask] = 0
            src_exposures[column].mask[src_exposures[column].mask] = False
        #
        # Some columns may not be masked, but we want to copy values from src_exposures anyway.
        #
        src_exposures_matched = src_exposures[column][src_exposures_index]
        dst_exposures_matched = dst_exposures_patched[column][dst_exposures_index]
        dst_exposures_mask_matched = np.zeros((len(dst_exposures_matched), ), dtype=bool)
        if hasattr(dst_exposures_patched[column], 'mask'):
            if np.any(dst_exposures_patched[column].mask[dst_exposures_index]):
                dst_exposures_mask_matched = dst_exposures_patched[column].mask[dst_exposures_index]
        else:
            if column == 'TILERA' or column == 'TILEDEC':
                dst_exposures_mask_matched = dst_exposures_bad_coord
            elif column == 'MJD':
                dst_exposures_mask_matched = (dst_exposures_patched['MJD'][dst_exposures_index] < 50000)
            else:
                assert column == 'SURVEY'
                dst_exposures_mask_matched = ((dst_exposures_patched['SURVEY'][dst_exposures_index] != 'cmx') &
                                              (dst_exposures_patched['SURVEY'][dst_exposures_index] != 'sv1') &
                                              (dst_exposures_patched['SURVEY'][dst_exposures_index] != 'sv2') &
                                              (dst_exposures_patched['SURVEY'][dst_exposures_index] != 'sv3') &
                                              (dst_exposures_patched['SURVEY'][dst_exposures_index] != 'main') &
                                              (dst_exposures_patched['SURVEY'][dst_exposures_index] != 'special'))
        if np.any(dst_exposures_mask_matched):
            log.info("Patching %d rows in dst_exposures column %s.",
                     np.sum(dst_exposures_mask_matched), column)
            dst_exposures_matched[dst_exposures_mask_matched] = src_exposures_matched[dst_exposures_mask_matched]
            dst_exposures_patched[column][dst_exposures_index] = dst_exposures_matched
            if hasattr(dst_exposures_patched[column], 'mask'):
                dst_exposures_matched.mask[dst_exposures_mask_matched] = False
                dst_exposures_patched[column].mask[dst_exposures_index] = dst_exposures_matched.mask
                #
                # Some values should have changed!
                #
                assert not (dst_exposures_patched[column].data.data == dst_exposures[column].data.data).all()
    #
    # QA checks.
    #
    assert not (dst_exposures_patched['TILERA'] == dst_exposures['TILERA']).all()
    assert not (dst_exposures_patched['TILEDEC'] == dst_exposures['TILEDEC']).all()
    assert not (dst_exposures_patched['MJD'] == dst_exposures['MJD']).all()
    assert not (dst_exposures_patched['SURVEY'] == dst_exposures['SURVEY']).all()
    assert (dst_exposures_patched['PROGRAM'] == dst_exposures['PROGRAM']).all()
    assert (dst_exposures_patched['FAPRGRM'] == dst_exposures['FAPRGRM']).all()
    assert (dst_exposures_patched['FAFLAVOR'] == dst_exposures['FAFLAVOR']).all()
    #
    # Patch missing MJD.
    #
    # We're only going to patch exposures that satisfy
    # EFFTIME_SPEC > 0 and NIGHT >= first_src_night *because*, empirically,
    # we know that we *can* obtain MJD from the raw data headers.
    # Outside of that range, that is not necessarily the case.
    #
    missing_mjd = ((dst_exposures_patched['MJD'] < 50000) &
                   (dst_exposures_patched['EFFTIME_SPEC'] > 0) &
                   (dst_exposures_patched['NIGHT'] >= first_night))
    for row in dst_exposures_patched[missing_mjd]:
        raw_data_file = os.path.join(os.environ['DESI_SPECTRO_DATA'],
                                     "{0:08d}".format(row['NIGHT']),
                                     "{0:08d}".format(row['EXPID']),
                                     "desi-{0:08d}.fits.fz".format(row['EXPID']))
        try:
            with fits.open(raw_data_file, mode='readonly') as hdulist:
                mjd_obs = hdulist['SPEC'].header['MJD-OBS']
            log.info("Tile %d exposure %d has MJD-OBS = %f in %s.", row['TILEID'], row['EXPID'], mjd_obs, raw_data_file)
            w = np.where(dst_exposures_patched['EXPID'] == row['EXPID'])[0]
            assert len(w) == 1
            dst_exposures_patched['MJD'][w] = mjd_obs
        except FileNotFoundError:
            log.error("%s not found, skipping patch!", raw_data_file)
    #
    # Fill any remaining masked values with zero.
    #
    dst_exposures_patched = zero_fill(dst_exposures_patched, 'exposures')
    return dst_exposures_patched


def patch_missing_frames_mjd(exposures, frames):
    """Update MJD values in `frames` after `exposures` has been patched.

    Parameters
    ----------
    exposures : :class:`~astropy.table.Table`
        Patched exposures table.
    frames : :class:`~astropy.table.Table`
        Patched frames table.

    Returns
    -------
    :class:`~astropy.table.Table`
        An updated version of `frames`.
    """
    log = get_logger()
    exposures_index, frames_index = match_rows(exposures['EXPID'], frames['EXPID'])
    exposures_mjd_matched = exposures['MJD'][exposures_index]
    frames_mjd_matched = frames['MJD'][frames_index]
    frames_missing_mjd = (exposures_mjd_matched != frames_mjd_matched) & (frames_mjd_matched < 50000)
    log.info("Patching %d frames with MJD == 0 from exposures.", np.sum(frames_missing_mjd))
    frames_mjd_matched[frames_missing_mjd] = exposures_mjd_matched[frames_missing_mjd]
    frames['MJD'][frames_index] = frames_mjd_matched
    assert (np.sum(frames['MJD'][frames_index] < 50000) ==
            np.sum((frames['MJD'][frames_index] < 50000) & (exposures['MJD'][exposures_index] < 50000)))
    log.warning("%d frames still have MJD == 0 because the corresponding exposures still have MJD == 0.",
                np.sum(frames['MJD'] < 50000))
    return frames


def patch_tiles(src_tiles, dst_tiles, timestamp):
    """Patch frames data in `dst_tiles` with the data in `src_tiles`.

    Parameters
    ----------
    src_tiles : :class:`~astropy.table.Table`
        Source of tiles data.
    dst_tiles : :class:`~astropy.table.Table`
        Data to be patched.
    timestamp : :class:`datetime.datetime`
        Fill value for the ``UPDATED`` column.

    Returns
    -------
    :class:`~astropy.table.Table`
        A *copy* of `dst_tiles` with data replaced from `src_tiles`.
    """
    log = get_logger()
    assert (np.unique(src_tiles['TILEID']) == sorted(src_tiles['TILEID'])).all()
    assert (np.unique(dst_tiles['TILEID']) == sorted(dst_tiles['TILEID'])).all()
    #
    # Patch TILERA, TILEDEC and other columns.
    #
    src_tiles_index, dst_tiles_index = match_rows(src_tiles['TILEID'], dst_tiles['TILEID'])
    dst_tiles_radec_matched = ((dst_tiles['TILERA'][dst_tiles_index] == 0) &
                               (dst_tiles['TILEDEC'][dst_tiles_index] == 0))
    dst_tiles_patched = dst_tiles.copy()
    for column in dst_tiles_patched.colnames:
        src_tiles_matched = src_tiles[column][src_tiles_index]
        dst_tiles_matched = dst_tiles_patched[column][dst_tiles_index]
        if column == 'TILERA' or column == 'TILEDEC':
            if np.any(dst_tiles_radec_matched):
                log.info("Patching %d rows in dst_tiles column %s.",
                         np.sum(dst_tiles_radec_matched), column)
                dst_tiles_matched[dst_tiles_radec_matched] = src_tiles_matched[dst_tiles_radec_matched]
                dst_tiles_patched[column][dst_tiles_index] = dst_tiles_matched
                assert not (dst_tiles_patched[column] == dst_tiles[column]).all()
        elif column in ('FAPRGRM', 'FAFLAVOR', 'OBSSTATUS', 'GOALTYPE'):
            dst_tiles_unknown_matched = dst_tiles_patched[column][dst_tiles_index] == 'unknown'
            if np.any(dst_tiles_unknown_matched):
                log.info("Patching %d rows in dst_tiles column %s.",
                         np.sum(dst_tiles_unknown_matched), column)
                dst_tiles_matched[dst_tiles_unknown_matched] = src_tiles_matched[dst_tiles_unknown_matched]
                dst_tiles_patched[column][dst_tiles_index] = dst_tiles_matched
                assert not (dst_tiles_patched[column] == dst_tiles[column]).all()
        else:
            if dst_tiles_patched[column].dtype.kind == 'f':
                dst_tiles_nan_matched = ~np.isfinite(dst_tiles_patched[column][dst_tiles_index])
                if np.any(dst_tiles_nan_matched):
                    log.info("Patching %d rows in dst_tiles column %s.",
                             np.sum(dst_tiles_nan_matched), column)
                    dst_tiles_matched[dst_tiles_nan_matched] = src_tiles_matched[dst_tiles_nan_matched]
                    dst_tiles_patched[column][dst_tiles_index] = dst_tiles_matched
                    assert not (dst_tiles_patched[column] == dst_tiles[column]).all()
    #
    # Patch SURVEY and PROGRAM.
    #
    dst_tiles_patched['PROGRAM'] = faflavor2program(dst_tiles_patched['FAFLAVOR'])
    oddball_survey = np.where((dst_tiles_patched['SURVEY'] != 'cmx') &
                              (dst_tiles_patched['SURVEY'] != 'sv1') &
                              (dst_tiles_patched['SURVEY'] != 'sv2') &
                              (dst_tiles_patched['SURVEY'] != 'sv3') &
                              (dst_tiles_patched['SURVEY'] != 'main') &
                              (dst_tiles_patched['SURVEY'] != 'special'))[0]
    oddball_program = np.where((dst_tiles_patched['PROGRAM'] != 'backup') &
                               (dst_tiles_patched['PROGRAM'] != 'bright') &
                               (dst_tiles_patched['PROGRAM'] != 'dark') &
                               (dst_tiles_patched['PROGRAM'] != 'other'))[0]
    assert (dst_tiles_patched['SURVEY'][oddball_survey] == 'unknown').all()
    assert len(oddball_program) == 0
    dst_tiles_patched['SURVEY'][oddball_survey] = 'cmx'
    #
    # Add UPDATED.
    #
    dst_tiles_patched['UPDATED'] = np.array([timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")]*len(dst_tiles_patched))
    #
    # QA check.
    #
    for column in dst_tiles_patched.colnames:
        assert np.isfinite(dst_tiles_patched[column]).all()
    return dst_tiles_patched


def get_data(options):
    """Read in source and destination data.

    Parameters
    ----------
    :class:`argparse.Namespace`
        The parsed command-line options.

    Returns
    -------
    :class:`tuple`
        A tuple containing two dictionaries, each containing three
        :class:`~astropy.table.Table` objects, plus some metadata.
    """
    src_tiles_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.src,
                                  f'tiles-{options.src}.csv')
    src_exposures_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.src,
                                      f'exposures-{options.src}.fits')
    dst_tiles_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.dst,
                                  f'tiles-{options.dst}.csv')
    dst_exposures_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.dst,
                                      f'exposures-{options.dst}.fits')
    src = {'tiles': Table.read(src_tiles_file, format='ascii.csv'),
           'tiles_file': src_tiles_file,
           'exposures': Table.read(src_exposures_file, format='fits', hdu='EXPOSURES'),
           'frames': Table.read(src_exposures_file, format='fits', hdu='FRAMES'),
           'exposures_file': src_exposures_file}
    dst = {'tiles': Table.read(dst_tiles_file, format='ascii.csv'),
           'tiles_file': dst_tiles_file,
           'exposures': Table.read(dst_exposures_file, format='fits', hdu='EXPOSURES'),
           'frames': Table.read(dst_exposures_file, format='fits', hdu='FRAMES'),
           'exposures_file': dst_exposures_file}
    return (src, dst)


def get_options():
    """Parse command-line options.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed options.
    """
    prsr = ArgumentParser(description='Patch top-level exposures and tiles summary files.',
                          prog=os.path.basename(sys.argv[0]))
    prsr.add_argument('-s', '--source', action='store', dest='src', metavar='SOURCE_SPECPROD', default='jura',
                      help='Use SOURCE_SPECPROD for the most correct data to apply in a patch (default "%(default)s").')
    prsr.add_argument('-d', '--destination', action='store', dest='dst', metavar='PATCH_SPECPROD', default='daily',
                      help='Apply data patches to PATCH_SPECPROD (default "%(default)s").')
    prsr.add_argument('-o', '--overwrite', action='store_true', dest='overwrite',
                      help='Overwrite any existing files in the output directory.')
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Print extra information.')
    prsr.add_argument('output', metavar='DIR', help='Write output to DIR.')
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
    options = get_options()
    #
    # Logging.
    #
    log_level = DEBUG if options.verbose else INFO
    log = get_logger(log_level)
    #
    # Read data files.
    #
    src, dst = get_data(options)
    #
    # Apply patches.
    #
    timestamp = datetime.datetime.now(tz=pytz.timezone('US/Pacific'))
    ymd = timestamp.strftime('%Y%m%d')
    patched = dict()
    patched['tiles_file'] = os.path.join(options.output, f'tiles-{options.dst}-patched-with-{options.src}-{ymd}.csv')
    patched['exposures_file'] = os.path.join(options.output, f'exposures-{options.dst}-patched-with-{options.src}-{ymd}.fits')
    patched['frames'] = patch_frames(src['frames'], dst['frames'])
    patched['exposures'] = patch_exposures(src['exposures'], dst['exposures'])
    patched['frames'] = patch_missing_frames_mjd(patched['exposures'], patched['frames'])
    patched['tiles'] = patch_tiles(src['tiles'], dst['tiles'], timestamp)
    #
    # Write out data.
    #
    dst_original_tiles = os.path.join(options.output, os.path.basename(dst['tiles_file']).replace(f"tiles-{options.dst}", f"tiles-{options.dst}-original-{ymd}"))
    dst_original_exposures = os.path.join(options.output, os.path.basename(dst['exposures_file']).replace(f"exposures-{options.dst}", f"exposures-{options.dst}-original-{ymd}"))
    for existing in (patched['tiles_file'],
                     patched['exposures_file'],
                     patched['exposures_file'].replace('.fits', '.csv'),
                     dst_original_tiles,
                     dst_original_exposures):
        if os.path.exists(existing):
            if options.overwrite:
                log.warning("%s exists and will be overwritten.", existing)
            else:
                log.error("%s exists and --overwrite was not specified.", existing)
                return 1
    if os.path.exists(dst_original_exposures) and options.overwrite:
        log.debug("os.remove('%s')", dst_original_exposures)
        os.remove(dst_original_exposures)
    log.debug("shutil.copy2('%s', '%s')", dst['exposures_file'], dst_original_exposures)
    copy2(dst['exposures_file'], dst_original_exposures)
    if os.path.exists(dst_original_tiles) and options.overwrite:
        log.debug("os.remove('%s')", dst_original_tiles)
        os.remove(dst_original_tiles)
    log.debug("shutil.copy2('%s', '%s')", dst['tiles_file'], dst_original_tiles)
    copy2(dst['tiles_file'], dst_original_tiles)
    patched['tiles'].write(patched['tiles_file'],
                           format='ascii.csv', overwrite=options.overwrite)
    patched['exposures'].write(patched['exposures_file'].replace('.fits', '.csv'),
                               format='ascii.csv', overwrite=options.overwrite)
    patched_exposures_hdulist = fits.HDUList([fits.PrimaryHDU(),
                                              fits.table_to_hdu(patched['exposures']),
                                              fits.table_to_hdu(patched['frames'])])
    patched_exposures_hdulist.writeto(patched['exposures_file'],
                                      overwrite=options.overwrite)
    return 0
