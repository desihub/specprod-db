# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.merge
================

Merge v2-style zcatalog files into an easily-ingestible set of intermediate files.

Notes
-----
* Obtain as much imaging/targeting/fiberassign information from zcatalog files
  as possible. Some fiberassign columns may be in the EXP_FIBERMAP files.
* Pre-assemble loaded data on SCRATCH, or load some columns from some files,
  other columns from other files.
* Additional matterhorn columns, like good_spec?
* matterhorn.fiberassign may need alternate q3c index?
* Can we just figure out a tag for matterhorn *tiles* in the next day or so?
* Read zall files, create intermediate files that have only the desired columns
  and have sky spectra removed, then merge the redshift-related intermediate files,
  but splitting out photometry & target information.
* While working on that, get the list of tiles, then extract fiberassign
  info from exp_fibermap files.
"""
import os
import sys
from argparse import ArgumentParser
import numpy as np
import pandas as pd
from astropy.io import fits
from desiutil.log import get_logger, DEBUG, INFO
from desispec.io import findfile
from .util import no_sky, programid, surveyid

#
# Origins for columns in each table.
#
column_sources = {'photometry': {'base': ['TARGETID', 'RA', 'DEC'],  # RA, DEC will be copied from TARGET_RA, TARGET_DEC
                                 'extra': [],
                                 'imaging': ['RELEASE', 'BRICKID', 'BRICKNAME', 'BRICK_OBJID', 'MORPHTYPE',
                                             'EBV', 'FLUX_G', 'FLUX_R', 'FLUX_Z', 'FLUX_IVAR_G', 'FLUX_IVAR_R', 'FLUX_IVAR_Z',
                                             'FLUX_W1', 'FLUX_W2', 'FLUX_IVAR_W1', 'FLUX_IVAR_W2',
                                             'FIBERFLUX_G', 'FIBERFLUX_R', 'FIBERFLUX_Z',
                                             'FIBERTOTFLUX_G', 'FIBERTOTFLUX_R', 'FIBERTOTFLUX_Z',
                                             'REF_EPOCH', 'MASKBITS',
                                             'SHAPE_R', 'SHAPE_E1', 'SHAPE_E2', 'SERSIC', 'REF_ID', 'REF_CAT',
                                             'GAIA_PHOT_G_MEAN_MAG', 'GAIA_PHOT_BP_MEAN_MAG', 'GAIA_PHOT_RP_MEAN_MAG',
                                             'PARALLAX', 'PMRA', 'PMDEC']},
                  'target': {'base': ['TARGETID', 'SURVEY', 'PROGRAM', 'TILEID',
                                      'CMX_TARGET', 'DESI_TARGET', 'BGS_TARGET', 'MWS_TARGET',
                                      'SV1_DESI_TARGET', 'SV1_BGS_TARGET', 'SV1_MWS_TARGET',
                                      'SV2_DESI_TARGET', 'SV2_BGS_TARGET', 'SV2_MWS_TARGET',
                                      'SV3_DESI_TARGET', 'SV3_BGS_TARGET', 'SV3_MWS_TARGET',
                                      'SCND_TARGET', 'SV1_SCND_TARGET', 'SV2_SCND_TARGET', 'SV3_SCND_TARGET'],
                             'extra': ['SUBPRIORITY', 'OBSCONDITIONS', 'PRIORITY_INIT', 'NUMOBS_INIT'],
                             'imaging': ['PHOTSYS']},
                  'fiberassign': {'base': ['TILEID', 'TARGETID', 'PETAL_LOC', 'FIBER',
                                           'FIBERSTATUS', 'TARGET_RA', 'TARGET_DEC',
                                           'FIBERASSIGN_X', 'FIBERASSIGN_Y', 'PRIORITY'],
                                  'extra': ['DEVICE_LOC', 'LOCATION', 'LAMBDA_REF', 'FA_TARGET', 'FA_TYPE', 'SUBPRIORITY'],
                                  'imaging': ['PMRA', 'PMDEC', 'REF_EPOCH', 'PARALLAX']},
                  'ztile': {'base': ['TARGETID', 'DESINAME', 'SURVEY', 'PROGRAM',
                                     'Z_BEST', 'ZERR_BEST', 'ZWARN_BEST', 'CHI2_BEST',
                                     'SPECTYPE_BEST', 'SUBTYPE_BEST', 'DELTACHI2_BEST',
                                     'COADD_FIBERSTATUS', 'TILEID',
                                     'COADD_NUMEXP', 'COADD_EXPTIME', 'COADD_NUMNIGHT', 'COADD_NUMTILE',
                                     'SV_NSPEC', 'SV_PRIMARY', 'MAIN_NSPEC', 'MAIN_PRIMARY',
                                     'ZCAT_NSPEC', 'ZCAT_PRIMARY', 'LASTNIGHT',
                                     'MIN_MJD', 'MEAN_MJD', 'MAX_MJD'],
                            'extra': ['COEFF', 'NPIXELS', 'NCOEFF', 'MEAN_DELTA_X', 'RMS_DELTA_X',
                                      'MEAN_DELTA_Y', 'RMS_DELTA_Y', 'MEAN_FIBER_RA', 'STD_FIBER_RA',
                                      'MEAN_FIBER_DEC', 'STD_FIBER_DEC', 'MEAN_PSF_TO_FIBER_SPECFLUX',
                                      'MEAN_FIBER_X', 'MEAN_FIBER_Y', 'TSNR2_GPBDARK_B', 'TSNR2_ELG_B',
                                      'TSNR2_GPBBRIGHT_B', 'TSNR2_LYA_B', 'TSNR2_BGS_B', 'TSNR2_GPBBACKUP_B',
                                      'TSNR2_QSO_B', 'TSNR2_LRG_B', 'TSNR2_GPBDARK_R', 'TSNR2_ELG_R',
                                      'TSNR2_GPBBRIGHT_R', 'TSNR2_LYA_R', 'TSNR2_BGS_R', 'TSNR2_GPBBACKUP_R',
                                      'TSNR2_QSO_R', 'TSNR2_LRG_R', 'TSNR2_GPBDARK_Z', 'TSNR2_ELG_Z',
                                      'TSNR2_GPBBRIGHT_Z', 'TSNR2_LYA_Z', 'TSNR2_BGS_Z', 'TSNR2_GPBBACKUP_Z',
                                      'TSNR2_QSO_Z', 'TSNR2_LRG_Z', 'TSNR2_GPBDARK', 'TSNR2_ELG',
                                      'TSNR2_GPBBRIGHT', 'TSNR2_LYA', 'TSNR2_BGS', 'TSNR2_GPBBACKUP',
                                      'TSNR2_QSO', 'TSNR2_LRG', 'FIRSTNIGHT'],
                            'imaging': []},
                  'zpix': {'base': ['TARGETID', 'DESINAME', 'SURVEY', 'PROGRAM', 'UNIQPIX',
                                    'Z_BEST', 'ZERR_BEST', 'ZWARN_BEST', 'CHI2_BEST',
                                    'SPECTYPE_BEST', 'SUBTYPE_BEST', 'DELTACHI2_BEST',
                                    'COADD_FIBERSTATUS',
                                    'CMX_TARGET', 'DESI_TARGET', 'BGS_TARGET', 'MWS_TARGET', 'SCND_TARGET',
                                    'SV1_DESI_TARGET', 'SV1_BGS_TARGET', 'SV1_MWS_TARGET', 'SV1_SCND_TARGET',
                                    'SV2_DESI_TARGET', 'SV2_BGS_TARGET', 'SV2_MWS_TARGET', 'SV2_SCND_TARGET',
                                    'SV3_DESI_TARGET', 'SV3_BGS_TARGET', 'SV3_MWS_TARGET', 'SV3_SCND_TARGET',
                                    'COADD_NUMEXP', 'COADD_EXPTIME', 'COADD_NUMNIGHT', 'COADD_NUMTILE',
                                    'SV_NSPEC', 'SV_PRIMARY', 'MAIN_NSPEC', 'MAIN_PRIMARY',
                                    'ZCAT_NSPEC', 'ZCAT_PRIMARY',
                                    'MIN_MJD', 'MEAN_MJD', 'MAX_MJD'],
                           'extra': ['COEFF', 'NPIXELS', 'NCOEFF', 'MEAN_DELTA_X', 'RMS_DELTA_X',
                                     'MEAN_DELTA_Y', 'RMS_DELTA_Y', 'MEAN_FIBER_RA', 'STD_FIBER_RA',
                                     'MEAN_FIBER_DEC', 'STD_FIBER_DEC', 'MEAN_PSF_TO_FIBER_SPECFLUX',
                                     'TSNR2_GPBDARK_B', 'TSNR2_ELG_B',
                                     'TSNR2_GPBBRIGHT_B', 'TSNR2_LYA_B', 'TSNR2_BGS_B', 'TSNR2_GPBBACKUP_B',
                                     'TSNR2_QSO_B', 'TSNR2_LRG_B', 'TSNR2_GPBDARK_R', 'TSNR2_ELG_R',
                                     'TSNR2_GPBBRIGHT_R', 'TSNR2_LYA_R', 'TSNR2_BGS_R', 'TSNR2_GPBBACKUP_R',
                                     'TSNR2_QSO_R', 'TSNR2_LRG_R', 'TSNR2_GPBDARK_Z', 'TSNR2_ELG_Z',
                                     'TSNR2_GPBBRIGHT_Z', 'TSNR2_LYA_Z', 'TSNR2_BGS_Z', 'TSNR2_GPBBACKUP_Z',
                                     'TSNR2_QSO_Z', 'TSNR2_LRG_Z', 'TSNR2_GPBDARK', 'TSNR2_ELG',
                                     'TSNR2_GPBBRIGHT', 'TSNR2_LYA', 'TSNR2_BGS', 'TSNR2_GPBBACKUP',
                                     'TSNR2_QSO', 'TSNR2_LRG'],
                           'imaging': []}}
table_to_extname = {'photometry': 'TRACTORPHOT',
                    'target': 'TARGETPHOT',
                    'ztile': 'ZCATALOG',
                    'zpix': 'ZCATALOG',
                    'fiberassign': 'FIBERASSIGN'}


def get_options():
    """Parse command-line options.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed options.
    """
    prsr = ArgumentParser(description=("Prepare merged FITS files for database loading."),
                          prog=os.path.basename(sys.argv[0]))
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-O', '--output', action='store', metavar='DIR',
                      default=os.environ['SCRATCH'],
                      help="Write files to DIR (default %(default)s).")
    prsr.add_argument('-o', '--overwrite', action='store_true',
                      help='Overwrite any existing files.')
    options = prsr.parse_args()
    return options


def detect_files(specprod, output):
    """Find any existing files and return anything that still needs to be
    processed.

    Parameters
    ----------
    specprod : :class:`str`
        Name of the specprod.
    output : :class:`str`
        Output directory.

    Returns
    -------
    :class:`tuple`
        Two lists of missing intermediate and final files.
    """
    intermediate_files = [os.path.join(output, f"{specprod}.{table}.temp.fits")
                          for table in column_sources.keys()]
    final_files = [os.path.join(output, f"{specprod}.{table}.fits")
                          for table in column_sources.keys()]
    missing_intermediate = list()
    missing_final = list()
    for f in intermediate_files:
        if not os.path.exists(f):
            missing_intermediate.append(os.path.basename(f).split('.')[1])
    for f in final_files:
        if not os.path.exists(f):
            missing_final.append(os.path.basename(f).split('.')[1])
    return (missing_intermediate, missing_final)


def main():
    """Entry-point for command-line scripts.

    Returns
    -------
    :class:`int`
        A value suitable for passing to :func:`sys.exit`.
    """
    options = get_options()
    if options.debug:
        log = get_logger(DEBUG, timestamp=True)
    else:
        log = get_logger(INFO, timestamp=True)
    specprod = os.environ['SPECPROD']
    zpix_file = findfile('zall_pix', version='v2', readonly=True)
    ztile_file = findfile('zall_tile', groupname='cumulative', version='v2', readonly=True)
    missing_intermediate, missing_final = detect_files(specprod, options.output)
    #
    # Step 1: rearrange columns and remove sky spectra.
    #
    rearrange_files = list()
    if any([table in missing_intermediate for table in ('photometry', 'target', 'fiberassign', 'ztile')]):
        rearrange_files.append(ztile_file)
    if 'zpix' in missing_intermediate:
        rearrange_files.append(zpix_file)
    if options.overwrite:
        rearrange_files = [ztile_file, zpix_file]
    for spec in rearrange_files:
        merge_columns = dict()
        for sub in ('base', 'extra', 'imaging'):
            if sub == 'base':
                src = spec
            else:
                src = spec.replace('.fits', f'-{sub}.fits')
            log.info('src = "%s"', src)
            with fits.open(src, character_as_bytes=True) as hdulist:
                catalog = hdulist[1].data
            good_rows = no_sky(catalog)
            if spec == ztile_file and sub == 'base':
                observed_tiles = np.unique(catalog['TILEID'])
                top_level_tiles = findfile('tiles', readonly=True)
                with fits.open(top_level_tiles) as hdulist:
                    tiles_catalog = hdulist[1].data
                itiles = tiles_catalog['TILEID'].argsort()
                assert (observed_tiles == tiles_catalog['TILEID'][itiles]).all()
            if spec == ztile_file:
                for merge_catalog in ('photometry', 'target', 'fiberassign', 'ztile'):
                    log.info('merge_catalog = "%s"', merge_catalog)
                    for column in column_sources[merge_catalog][sub]:
                        log.debug('column = "%s"', column)
                        if merge_catalog == 'photometry' and (column == 'RA' or column == 'DEC'):
                            new_column = catalog.columns[f'TARGET_{column}'].copy()
                            new_column.name = column
                        elif merge_catalog == 'fiberassign' and column == 'FIBERSTATUS':
                            new_column = catalog.columns['COADD_FIBERSTATUS'].copy()
                            new_column.name = column
                            new_column.array = (catalog['COADD_FIBERSTATUS'] & 8)
                        else:
                            new_column = catalog.columns[column].copy()
                        new_column.array = new_column.array[good_rows].copy()
                        if merge_catalog in merge_columns:
                            merge_columns[merge_catalog].append(new_column)
                        else:
                            merge_columns[merge_catalog] = [new_column]
            if spec == zpix_file:
                merge_catalog = 'zpix'
                log.info('merge_catalog = "%s"', merge_catalog)
                for column in column_sources[merge_catalog][sub]:
                    log.debug('column = "%s"', column)
                    new_column = catalog.columns[column].copy()
                    new_column.array = new_column.array[good_rows].copy()
                    if merge_catalog in merge_columns:
                        merge_columns[merge_catalog].append(new_column)
                    else:
                        merge_columns[merge_catalog] = [new_column]
        for table in merge_columns:
            output_file = os.path.join(options.output, f"{specprod}.{table}.temp.fits")
            log.info(output_file)
            log.debug([c.name for c in merge_columns[table]])
            hdu = fits.BinTableHDU.from_columns(merge_columns[table],
                                                name=table_to_extname[table],
                                                character_as_bytes=True)
            hdu.writeto(output_file, overwrite=options.overwrite)
    #
    # Step 2: remove duplicate entries.
    #
    if options.overwrite:
        missing_final = list(column_sources.keys())
    if missing_final:
        for table in missing_final:
            table_file = os.path.join(options.output, f"{specprod}.{table}.temp.fits")
            log.info(table_file)
            with fits.open(table_file, character_as_bytes=True) as hdulist:
                table_data = hdulist[1].data
                table_header = hdulist[1].header  # copy EXTNAME
            log.info('len(table_data) = %d', len(table_data))
            if table == 'photometry':
                id_array = np.zeros((len(table_data), 1), dtype=np.int64)
            else:
                id_array = np.zeros((len(table_data), 3), dtype=np.int64)
            id_array[:, 0] = table_data['TARGETID']
            if table == 'target':
                id_array[:, 1] = table_data['TILEID']
                id_array[:, 2] = np.array([surveyid(s) for s in table_data['SURVEY'].tolist()], dtype=np.int64)
            elif table == 'fiberassign':
                id_array[:, 1] = table_data['TILEID']
                id_array[:, 2] = table_data['LOCATION']
            elif table == 'ztile':
                id_array[:, 1] = table_data['TILEID']
                id_array[:, 2] = table_data['LASTNIGHT']
            elif table == 'zpix':
                id_array[:, 1] = np.array([surveyid(s) for s in table_data['SURVEY'].tolist()], dtype=np.int64)
                id_array[:, 2] = np.array([programid(p) for p in table_data['PROGRAM'].tolist()], dtype=np.int64)
            else:
                pass
            unique_array, good_rows = np.unique(id_array, return_index=True, axis=0)
            log.info('len(good_rows) = %d', len(good_rows))
            hdu = fits.BinTableHDU(table_data[good_rows], name=table_header['EXTNAME'], character_as_bytes=True)
            output_file = os.path.join(options.output, f"{specprod}.{table}.fits")
            log.info(output_file)
            hdu.writeto(output_file, overwrite=options.overwrite)
    return 0
