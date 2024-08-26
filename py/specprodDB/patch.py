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
from argparse import ArgumentParser
import numpy as np
from astropy.table import Table, join
from astropy.io import fits
from desiutil.log import get_logger, DEBUG, INFO
# from desispec.io import read_table
from desispec.io.meta import faflavor2program
from specprodDB.util import cameraid


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
    src_frames_join = Table()
    src_frames_join['FRAMEID'] = np.array([100*row['EXPID'] + cameraid(row['CAMERA']) for row in src_frames])
    src_frames_join['SRC_INDEX'] = np.arange(len(src_frames))
    dst_frames_join = Table()
    dst_frames_join['FRAMEID'] = np.array([100*row['EXPID'] + cameraid(row['CAMERA']) for row in dst_frames])
    dst_frames_join['DST_INDEX'] = np.arange(len(dst_frames))
    joined_frames = join(src_frames_join, dst_frames_join, join_type='outer', keys='FRAMEID')
    src_frames_index = joined_frames[(~joined_frames['SRC_INDEX'].mask) & (~joined_frames['DST_INDEX'].mask)]['SRC_INDEX']
    dst_frames_index = joined_frames[(~joined_frames['SRC_INDEX'].mask) & (~joined_frames['DST_INDEX'].mask)]['DST_INDEX']
    dst_frames_patched = dst_frames.copy()
    for column in dst_frames_patched.colnames:
        if hasattr(dst_frames_patched[column], 'mask') and not column.startswith('TSNR2_'):
            log.info("Patching frames column %s.", column)
            dst_frames_patched[column][dst_frames_index] = src_frames[column][src_frames_index]
            dst_frames_patched[column].mask[dst_frames_index] = False
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
    src_exposures_join = Table()
    src_exposures_join['EXPID'] = src_exposures['EXPID']
    src_exposures_join['SRC_INDEX'] = np.arange(len(src_exposures))
    dst_exposures_join = Table()
    dst_exposures_join['EXPID'] = dst_exposures['EXPID']
    dst_exposures_join['DST_INDEX'] = np.arange(len(dst_exposures))
    joined_exposures = join(src_exposures_join, dst_exposures_join, join_type='outer', keys='EXPID')
    src_exposures_index = joined_exposures[(~joined_exposures['SRC_INDEX'].mask) & (~joined_exposures['DST_INDEX'].mask)]['SRC_INDEX']
    dst_exposures_index = joined_exposures[(~joined_exposures['SRC_INDEX'].mask) & (~joined_exposures['DST_INDEX'].mask)]['DST_INDEX']
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
    for column in ['TILERA', 'TILEDEC', 'MJD', 'SURVEY'] + [c for c in dst_exposures_patched.colnames if hasattr(dst_exposures_patched[c], 'mask') and c in can_patch]:
        log.info("Patching exposures column %s.", column)
        if hasattr(src_exposures[column], 'mask'):
            if np.any(src_exposures[column].mask[src_exposures_index]):
                src_exposures[column][src_exposures[column].mask] = 0
                src_exposures[column].mask[src_exposures[column].mask] = False
        dst_exposures_patched[column][dst_exposures_index] = src_exposures[column][src_exposures_index]
        if hasattr(dst_exposures_patched[column], 'mask'):
            dst_exposures_patched[column].mask[dst_exposures_index] = False
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
    missing_mjd = np.where((dst_exposures_patched['NIGHT'] >= first_night) &
                           (dst_exposures_patched['EFFTIME_SPEC'] > 0) &
                           (dst_exposures_patched['MJD'] < 50000))[0]
    for row in dst_exposures_patched[missing_mjd]:
        raw_data_file = os.path.join(os.environ['DESI_SPECTRO_DATA'],
                                     "{0:08d}".format(row['NIGHT']),
                                     "{0:08d}".format(row['EXPID']),
                                     "desi-{0:08d}.fits.fz".format(row['EXPID']))
        with fits.open(raw_data_file, mode='readonly') as hdulist:
            mjd_obs = hdulist['SPEC'].header['MJD-OBS']
        log.info("Tile %d exposure %d has MJD-OBS = %f in %s.", row['TILEID'], row['EXPID'], mjd_obs, raw_data_file)
        w = np.where(dst_exposures_patched['EXPID'] == row['EXPID'])[0]
        assert len(w) == 1
        dst_exposures_patched['MJD'][w] = mjd_obs
    #
    # Fill any remaining masked values with zero.
    #
    for c in dst_exposures_patched.colnames:
        if hasattr(dst_exposures_patched[c], 'mask'):
            if dst_exposures_patched[c].mask.any():
                dst_exposures_patched[c][dst_exposures_patched[c].mask] = 0
                dst_exposures_patched[c].mask[dst_exposures_patched[c].mask] = False
    return dst_exposures_patched


def patch_tiles(src_tiles, dst_tiles):
    """Patch frames data in `dst_tiles` with the data in `src_tiles`.

    Parameters
    ----------
    src_tiles : :class:`~astropy.table.Table`
        Source of tiles data.
    dst_tiles : :class:`~astropy.table.Table`
        Data to be patched.

    Returns
    -------
    :class:`~astropy.table.Table`
        A *copy* of `dst_tiles` with data replaced from `src_tiles`.
    """
    assert (np.unique(src_tiles['TILEID']) == sorted(src_tiles['TILEID'])).all()
    assert (np.unique(dst_tiles['TILEID']) == sorted(dst_tiles['TILEID'])).all()
    dst_tiles_patched = dst_tiles.copy()
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
    src_tiles_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.src, f'tiles-{options.src}.csv')
    src_exposures_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.src, f'exposures-{options.src}.fits')
    dst_tiles_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.dst, f'tiles-{options.dst}.csv')
    dst_exposures_file = os.path.join(os.environ['DESI_SPECTRO_REDUX'], options.dst, f'exposures-{options.dst}.fits')
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
    prsr.add_argument('-o', '--overwrite', action='store_true', dst='overwrite',
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
    global log
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
    timestamp = datetime.date.today().strftime('%Y%m%d')
    patched = dict()
    patched['tiles_file'] = os.path.join(options.output, f'tiles-{options.dst}-patched-with-{options.src}-{timestamp}.csv')
    patched['exposures_file'] = os.path.join(options.output, f'exposures-{options.dst}-patched-with-{options.src}-{timestamp}.fits')
    patched['frames'] = patch_frames(src['frames'], dst['frames'])
    patched['exposures'] = patch_exposures(src['exposures'], dst['exposures'])
    patched['tiles'] = patch_tiles(src['tiles'], dst['tiles'])
    #
    # Write out data.
    #
    for existing in (patched['tiles_file'], patched['exposures_file'], patched['exposures_file'].replace('.fits', '.csv')):
        if os.path.exists(existing):
            if options.overwrite:
                log.warning("%s exists and will be overwritten.", existing)
            else:
                log.error("%s exists and --overwrite was not specified.", existing)
                return 1
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