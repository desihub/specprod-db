# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.coeff
================

Patch redrock template coefficients. This is meant to fix bad values in the
``zpix`` and ``ztile`` tables in the ``coeff_0`` ... ``coeff_9`` columns.
This affected ``fuji``, ``guadalupe`` and ``iron``, but not ``loa``.
"""
import os
import sys
import re
from argparse import ArgumentParser
from astropy.table import Table
from desiutil.log import get_logger, DEBUG, INFO
from desiutil.names import radec_to_desiname
# from desispec.io import findfile
from . import __version__ as specprodDB_version
from .util import no_sky


log = None


def get_options(description='Extract coefficient columns to create a coeff patch table.'):
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
    prsr = ArgumentParser(description=description,
                          prog=os.path.basename(sys.argv[0]))
    prsr.add_argument('-o', '--overwrite', action='store_true',
                      help='Overwrite any existing files.')
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Print extra information.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s ' + specprodDB_version)
    prsr.add_argument('zall', metavar='FILE', help='Read coefficients from FILE.')
    return prsr.parse_args()


def copy_columns(catalog, rowfilter, catalog_type):
    """Copy columns from `catalog` into a new table.

    Parameters
    ----------
    catalog : :class:`~astropy.table.Table`
        Input data table.
    rowfilter : callable
        A function that returns the subset of rows to include from `catalog`.
    catalog_type : :class:`str`
        Indicates the set of columns contained in `catalog`.

    Returns
    -------
    :class:`~astropy.table.Table`
        A new Table with the desired columns.
    """
    cols = {'pix': ('TARGETID', 'SURVEY', 'PROGRAM', 'HEALPIX', 'DESINAME'),
            'tilecumulative': ('TARGETID', 'TILEID', 'SPGRPVAL', 'DESINAME')}
    good_rows = rowfilter(catalog)
    new_table = Table()
    new_table.meta['EXTNAME'] = 'COEFF_PATCH'
    for column in cols[catalog_type]:
        if column in catalog.colnames:
            new_table[column] = catalog[column][good_rows].copy()
        else:
            new_table[column] = radec_to_desiname(catalog['TARGET_RA'][good_rows],
                                                  catalog['TARGET_DEC'][good_rows])
    for k in range(10):
        new_table[f'COEFF_{k:d}'] = catalog['COEFF'][good_rows, k].copy()
    return new_table


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    global log
    options = get_options()
    #
    # Logging
    #
    if options.verbose:
        log = get_logger(DEBUG, timestamp=True)
    else:
        log = get_logger(INFO, timestamp=True)
    # specprod = os.environ['SPECPROD']
    patch_dir = os.path.join(os.environ['SCRATCH'], 'coeff_patch')
    if not os.path.isdir(patch_dir):
        log.debug("os.makedirs('%s', exist_ok=True)", patch_dir)
        os.makedirs(patch_dir, exist_ok=True)
    if not os.path.exists(options.zall):
        log.critical("Could not find %s!", options.zall)
        return 1
    zall_filename = os.path.basename(options.zall)
    m = re.match(r'zall-(pix|tilecumulative)-([a-z]+)\.fits', zall_filename)
    if m:
        catalog_type, specprod = m.groups()
    else:
        log.critical("Could not match catalog type for %s!", zall_filename)
        return 1
    zall_table = Table.read(options.zall, hdu='ZCATALOG')
    patch_table = copy_columns(zall_table, no_sky, catalog_type)
    patch_table_name = os.path.join(patch_dir,
                                    zall_filename.replace('.fits',
                                                          '-coeff-patch.fits'))
    log.debug("patch_table.write('%s', overwrite=%s, checksum=True)",
              patch_table_name, options.overwrite)
    patch_table.write(patch_table_name, overwrite=options.overwrite, checksum=True)
    return 0
