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
from desispec.io import findfile
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
    good_spectra = no_sky(zall_table)
    patch_table = Table()
    patch_table['TARGETID'] = zall_table['TARGETID'][good_spectra].copy()
    if catalog_type == 'pix':
        patch_table['SURVEY'] = zall_table['SURVEY'][good_spectra].copy()
        patch_table['PROGRAM'] = zall_table['PROGRAM'][good_spectra].copy()
    else:
        patch_table['TILEID'] = zall_table['PROGRAM'][good_spectra].copy()
    for k in range(10):
        patch_table[f'COEFF_{k:d}'] = zall_table['COEFF'][good_spectra, k].copy()
    patch_table_name = os.path.join(patch_dir,
                                    zall_filename.replace('.fits',
                                                          '-coeff-patch.fits'))
    log.debug("patch_table.write('%s', overwrite=%s)",
              patch_table_name, options.overwrite)
    patch_table.write(patch_table_name, overwrite=options.overwrite)
    return 0
