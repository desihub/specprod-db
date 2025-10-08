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
# import itertools
from argparse import ArgumentParser
from configparser import ConfigParser

from sqlalchemy import (Column, BigInteger, Integer, String, Numeric)
from sqlalchemy.orm import declared_attr
from sqlalchemy.schema import Index
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.dialects.postgresql import insert as pg_insert

import numpy as np

from astropy.table import Table

from desiutil.log import get_logger, DEBUG, INFO
from desiutil.names import radec_to_desiname
# from desispec.io import findfile

from . import __version__ as specprodDB_version
from .load import SchemaMixin, Base, finitize, setup_db, load_file
from .load import log as db_log
from .util import no_sky, programid, surveyid, spgrpid, common_options


log = None


class ZpixPatch(SchemaMixin, Base):
    """Table for patching Zpix table.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "survey", "program", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetid = Column(BigInteger, nullable=False, index=True)
    desiname = Column(String(22), nullable=False, index=True)
    survey = Column(String(7), nullable=False, index=True)
    program = Column(String(6), nullable=False, index=True)
    healpix = Column(Integer, nullable=False, index=True)
    coeff_0 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_1 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_2 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_3 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_4 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_5 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_6 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_7 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_8 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_9 = Column(DOUBLE_PRECISION, nullable=False)

    def __repr__(self):
        return "ZpixPatch(targetid={0.targetid:d}, survey='{0.survey}', program='{0.program}')".format(self)

    @classmethod
    def convert(cls, data, survey=None, program=None, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        survey : :class:`str`, optional
            Survey name. If not set, it will be obtained from `data`.
        program : :class:`str`, optional
            Program name. If not set, it will be obtained from `data`.
        row_index : :class:`numpy.ndarray`, optional
            Only convert the rows indexed by `row_index`. If not specified,
            convert all rows.

        Returns
        -------
        :class:`list`
            A list of ORM objects.
        """
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        default_columns = dict()
        #
        # Reductions like guadalupe may not have the full set of target bitmasks
        #
        check_columns = {'survey': survey, 'program': program}
        for column in check_columns:
            if check_columns[column] is None:
                if column.upper() in data.colnames:
                    log.info("Obtaining '%s' from input data file.", column)
                else:
                    msg = "Could not obtain '%s' from input data file."
                    log.critical(msg, column)
                    raise KeyError(msg % (column, ))
            else:
                default_columns[column] = check_columns[column]
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'id':
                if 'survey' in default_columns:
                    id0 = programid(program) << 32 | surveyid(survey)
                else:
                    s = np.array([surveyid(s) for s in data['SURVEY']], dtype=np.int64)
                    p = np.array([programid(s) for s in data['PROGRAM']], dtype=np.int64)
                    id0 = p << 32 | s
                data_column = [(i0 << 64) | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'][row_index].tolist())]
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class ZtilePatch(SchemaMixin, Base):
    """Table for patching Ztile table.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "tileid", "spgrpval", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetid = Column(BigInteger, nullable=False, index=True)
    tileid = Column(Integer, nullable=False, index=True)
    spgrpval = Column(Integer, nullable=False, index=True)
    desiname = Column(String(22), nullable=False, index=True)
    coeff_0 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_1 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_2 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_3 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_4 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_5 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_6 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_7 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_8 = Column(DOUBLE_PRECISION, nullable=False)
    coeff_9 = Column(DOUBLE_PRECISION, nullable=False)

    def __repr__(self):
        return "ZtilePatch(targetid={0.targetid:d}, tileid={0.tileid:d}, spgrpval={0.spgrpval:d})".format(self)

    @classmethod
    def convert(cls, data, tileid=None, night=None, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        tileid : :class:`int`, optional
            Tile ID number. If not set, it will be obtained from `data`.
        night : :class:`int`, optional
            Night number. This is loaded into the ``firstnight`` column.
            If not set, it will be obtained from `data`.
        row_index : :class:`numpy.ndarray`, optional
            Only convert the rows indexed by `row_index`. If not specified,
            convert all rows.

        Returns
        -------
        :class:`list`
            A list of ORM objects.
        """
        spgrp = 'cumulative'
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        default_columns = dict()
        check_columns = {'tileid': tileid, 'spgrpval': night}
        for column in check_columns:
            if check_columns[column] is None:
                if column.upper() in data.colnames:
                    log.info("Obtaining '%s' from input data file.", column)
                else:
                    msg = "Could not obtain '%s' from input data file."
                    log.critical(msg, column)
                    raise KeyError(msg % (column, ))
            else:
                default_columns[column] = check_columns[column]
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'id':
                if 'survey' in default_columns:
                    id0 = ((spgrpid(spgrp) << 27 | data['SPGRPVAL'][row_index].base.astype(np.int64)) << 32) | tileid
                else:
                    id0 = ((spgrpid(spgrp) << 27 | data['SPGRPVAL'][row_index].base.astype(np.int64)) << 32) | data['TILEID'][row_index].astype(np.int64)
                data_column = [(i0 << 64) | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'][row_index].tolist())]
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


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
    prsr = common_options(description)
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
    db_log = log
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
    patch_table_name = os.path.join(patch_dir,
                                    zall_filename.replace('.fits',
                                                          '-coeff-patch.fits'))
    if os.path.exists(patch_table_name) and not options.overwrite:
        log.info("Patch file, %s, detected, skipping step.", patch_table_name)
    else:
        zall_table = Table.read(options.zall, hdu='ZCATALOG')
        patch_table = copy_columns(zall_table, no_sky, catalog_type)
        log.debug("patch_table.write('%s', overwrite=%s, checksum=True)",
                  patch_table_name, options.overwrite)
        patch_table.write(patch_table_name, overwrite=options.overwrite)  # , checksum=True)
    #
    # Read configuration file.
    #
    config = ConfigParser()
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
    postgresql = setup_db(hostname=config[specprod]['hostname'],
                          username=config[specprod]['username'],
                          schema='coeff_patch',
                          overwrite=options.overwrite,
                          verbose=options.verbose)
    #
    # Loading
    #
    if catalog_type == 'pix':
        tcls = ZpixPatch
    else:
        tcls = ZtilePatch
    log.info("Loading %s from %s.", tcls.__tablename__, patch_table_name)
    load_file(patch_table_name, tcls, alternate_load=True)
    log.info("Finished loading %s.", tcls.__tablename__)
    return 0
