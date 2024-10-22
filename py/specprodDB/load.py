# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.load
===============

Code for loading a spectroscopic production database. This includes both
targeting and redshift data.
"""
import os
# import re
import glob
import itertools
# import sys
import importlib.resources as ir
from configparser import SafeConfigParser

import numpy as np
from astropy import __version__ as astropy_version
# from astropy.io import fits
from astropy.table import Table, MaskedColumn, join
from astropy.time import Time
from pytz import utc

from sqlalchemy import __version__ as sqlalchemy_version
from sqlalchemy import (create_engine, event, ForeignKey, Column, DDL,
                        BigInteger, Boolean, Integer, String, DateTime,
                        SmallInteger, Numeric, text)
from sqlalchemy.orm import (DeclarativeBase, declarative_mixin, declared_attr,
                            scoped_session, sessionmaker, relationship)
from sqlalchemy.schema import Index
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, REAL
from sqlalchemy.dialects.postgresql import insert as pg_insert

from desiutil import __version__ as desiutil_version
from desiutil.iers import freeze_iers
from desiutil.names import radec_to_desiname
from desiutil.log import get_logger, DEBUG, INFO

# from desispec.io.meta import findfile

from . import __version__ as specprodDB_version
from .util import (common_options, parse_pgpass, cameraid, surveyid, programid,
                   spgrpid, checkgzip, no_sky)


# Base = declarative_base()
engine = None
dbSession = scoped_session(sessionmaker())
schemaname = None
log = None


class Base(DeclarativeBase):
    """SQLAlchemy 2.0 replacement for ``Base = declarative_base()``.
    """
    pass


@declarative_mixin
class SchemaMixin(object):
    """Mixin class to allow schema name to be changed at runtime. Also
    automatically sets the table name.
    """

    @declared_attr.directive
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr.directive
    def __table_args__(cls):
        return {'schema': schemaname}


class Version(SchemaMixin, Base):
    """Store package version metadata.
    """
    id = Column(Integer, primary_key=True, autoincrement=True)
    package = Column(String(20), nullable=False, unique=True)
    version = Column(String(20), nullable=False)

    def __repr__(self):
        return "Version(package='{0.package}', version='{0.version}')".format(self)

    @classmethod
    def convert(cls, data, row_index=None):
        """Convert the inputs into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        row_index: :class:`numpy.ndarray`, optional
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
        #
        # Version has no floating-point columns.
        #
        # data = finitize(data)
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'id':
                data_column = (row_index + 1).tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Photometry(SchemaMixin, Base):
    """Contains *only* photometric quantities associated with a ``TARGETID``.

    This table is deliberately designed so that ``TARGETID`` can serve as a
    primary key. Any quantities created or modified by desitarget are
    defined in the :class:`~specprodDB.load.Target` class.

    However we *avoid* the use of the term "tractor" for this table,
    because not every target will have *tractor* photometry,

    Notes
    -----
    The various ``LC`` (light curve) columns,
    which are vector-valued, are not yet implemented.
    """
    ls_id = Column(BigInteger, nullable=False, index=True)  # (release << 40) | (brickid << 16) | brick_objid
    release = Column(SmallInteger, nullable=False)  # targetphot, fiberassign
    brickid = Column(Integer, nullable=False)  # targetphot, fiberassign
    brickname = Column(String(8), nullable=False)  # targetphot, fiberassign
    brick_objid = Column(Integer, nullable=False)  # targetphot, fiberassign
    morphtype = Column(String(4), nullable=False)  # targetphot, fiberassign
    ra = Column(DOUBLE_PRECISION, nullable=False)  # targetphot, fiberassign: target_ra?
    ra_ivar = Column(REAL, nullable=False)  # targetphot
    dec = Column(DOUBLE_PRECISION, nullable=False)  # targetphot, fiberassign: target_dec?
    dec_ivar = Column(REAL, nullable=False)  # targetphot
    dchisq_psf = Column(REAL, nullable=False)  # targetphot
    dchisq_rex = Column(REAL, nullable=False)  # targetphot
    dchisq_dev = Column(REAL, nullable=False)  # targetphot
    dchisq_exp = Column(REAL, nullable=False)  # targetphot
    dchisq_ser = Column(REAL, nullable=False)  # targetphot
    ebv = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_g = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_r = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_z = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_ivar_g = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_ivar_r = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_ivar_z = Column(REAL, nullable=False)  # targetphot, fiberassign
    mw_transmission_g = Column(REAL, nullable=False)  # targetphot
    mw_transmission_r = Column(REAL, nullable=False)  # targetphot
    mw_transmission_z = Column(REAL, nullable=False)  # targetphot
    fracflux_g = Column(REAL, nullable=False)  # targetphot
    fracflux_r = Column(REAL, nullable=False)  # targetphot
    fracflux_z = Column(REAL, nullable=False)  # targetphot
    fracmasked_g = Column(REAL, nullable=False)  # targetphot
    fracmasked_r = Column(REAL, nullable=False)  # targetphot
    fracmasked_z = Column(REAL, nullable=False)  # targetphot
    fracin_g = Column(REAL, nullable=False)  # targetphot
    fracin_r = Column(REAL, nullable=False)  # targetphot
    fracin_z = Column(REAL, nullable=False)  # targetphot
    nobs_g = Column(SmallInteger, nullable=False)  # targetphot
    nobs_r = Column(SmallInteger, nullable=False)  # targetphot
    nobs_z = Column(SmallInteger, nullable=False)  # targetphot
    psfdepth_g = Column(REAL, nullable=False)  # targetphot
    psfdepth_r = Column(REAL, nullable=False)  # targetphot
    psfdepth_z = Column(REAL, nullable=False)  # targetphot
    galdepth_g = Column(REAL, nullable=False)  # targetphot
    galdepth_r = Column(REAL, nullable=False)  # targetphot
    galdepth_z = Column(REAL, nullable=False)  # targetphot
    flux_w1 = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_w2 = Column(REAL, nullable=False)  # targetphot, fiberassign
    flux_w3 = Column(REAL, nullable=False)  # targetphot
    flux_w4 = Column(REAL, nullable=False)  # targetphot
    flux_ivar_w1 = Column(REAL, nullable=False)  # fiberassign
    flux_ivar_w2 = Column(REAL, nullable=False)  # fiberassign
    flux_ivar_w3 = Column(REAL, nullable=False)  # targetphot
    flux_ivar_w4 = Column(REAL, nullable=False)  # targetphot
    mw_transmission_w1 = Column(REAL, nullable=False)  # targetphot
    mw_transmission_w2 = Column(REAL, nullable=False)  # targetphot
    mw_transmission_w3 = Column(REAL, nullable=False)  # targetphot
    mw_transmission_w4 = Column(REAL, nullable=False)  # targetphot
    allmask_g = Column(SmallInteger, nullable=False)  # targetphot
    allmask_r = Column(SmallInteger, nullable=False)  # targetphot
    allmask_z = Column(SmallInteger, nullable=False)  # targetphot
    fiberflux_g = Column(REAL, nullable=False)  # targetphot, fiberassign
    fiberflux_r = Column(REAL, nullable=False)  # targetphot, fiberassign
    fiberflux_z = Column(REAL, nullable=False)  # targetphot, fiberassign
    fibertotflux_g = Column(REAL, nullable=False)  # targetphot, fiberassign
    fibertotflux_r = Column(REAL, nullable=False)  # targetphot, fiberassign
    fibertotflux_z = Column(REAL, nullable=False)  # targetphot, fiberassign
    ref_epoch = Column(REAL, nullable=False)  # targetphot, fiberassign
    wisemask_w1 = Column(SmallInteger, nullable=False)  # targetphot
    wisemask_w2 = Column(SmallInteger, nullable=False)  # targetphot
    maskbits = Column(SmallInteger, nullable=False)  # targetphot, fiberassign
    # LC_...
    shape_r = Column(REAL, nullable=False)  # targetphot, fiberassign
    shape_e1 = Column(REAL, nullable=False)  # targetphot, fiberassign
    shape_e2 = Column(REAL, nullable=False)  # targetphot, fiberassign
    shape_r_ivar = Column(REAL, nullable=False)  # targetphot
    shape_e1_ivar = Column(REAL, nullable=False)  # targetphot
    shape_e2_ivar = Column(REAL, nullable=False)  # targetphot
    sersic = Column(REAL, nullable=False)  # targetphot, fiberassign
    sersic_ivar = Column(REAL, nullable=False)  # targetphot
    ref_id = Column(BigInteger, nullable=False)  # targetphot, fiberassign
    ref_cat = Column(String(2), nullable=False)  # targetphot, fiberassign
    gaia_phot_g_mean_mag = Column(REAL, nullable=False)  # targetphot, fiberassign
    gaia_phot_g_mean_flux_over_error = Column(REAL, nullable=False)  # targetphot
    gaia_phot_bp_mean_mag = Column(REAL, nullable=False)  # targetphot, fiberassign
    gaia_phot_bp_mean_flux_over_error = Column(REAL, nullable=False)  # targetphot
    gaia_phot_rp_mean_mag = Column(REAL, nullable=False)  # targetphot, fiberassign
    gaia_phot_rp_mean_flux_over_error = Column(REAL, nullable=False)  # targetphot
    gaia_phot_bp_rp_excess_factor = Column(REAL, nullable=False)  # targetphot
    gaia_duplicated_source = Column(Boolean, nullable=False)  # targetphot
    gaia_astrometric_sigma5d_max = Column(REAL, nullable=False)  # targetphot
    gaia_astrometric_params_solved = Column(SmallInteger, nullable=False)  # targetphot, but inconsistent type!
    parallax = Column(REAL, nullable=False)  # targetphot, fiberassign
    parallax_ivar = Column(REAL, nullable=False)  # targetphot
    pmra = Column(REAL, nullable=False)  # targetphot, fiberassign
    pmra_ivar = Column(REAL, nullable=False)  # targetphot
    pmdec = Column(REAL, nullable=False)  # targetphot, fiberassign
    pmdec_ivar = Column(REAL, nullable=False)  # targetphot
    targetid = Column(BigInteger, primary_key=True, autoincrement=False)  # targetphot, fiberassign

    targets = relationship("Target", back_populates="photometry")
    fiberassign = relationship("Fiberassign", back_populates="photometry")
    potential = relationship("Potential", back_populates="photometry")
    zpix_redshifts = relationship("Zpix", back_populates="photometry")
    ztile_redshifts = relationship("Ztile", back_populates="photometry")

    def __repr__(self):
        return "Photometry(targetid={0.targetid:d})".format(self)

    @classmethod
    def convert(cls, data, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        row_index: :class:`numpy.ndarray`, optional
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
        expand_dchisq = ('dchisq_psf', 'dchisq_rex', 'dchisq_dev', 'dchisq_exp', 'dchisq_ser',)
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'brick_objid' and 'BRICK_OBJID' not in data.colnames:
                data_column = data['OBJID'][row_index].tolist()
            elif column.name == 'morphtype' and 'MORPHTYPE' not in data.colnames:
                data_column = data['TYPE'][row_index].tolist()
            elif column.name == 'ls_id' and 'LS_ID' not in data.colnames:
                data_column = ((data[row_index]['RELEASE'].data.astype(np.int64) << 40) |
                               (data[row_index]['BRICKID'].data.astype(np.int64) << 16) |
                               (data[row_index]['BRICK_OBJID'].data.astype(np.int64))).tolist()
            elif column.name == 'gaia_astrometric_params_solved' and data[column.name.upper()].dtype.kind != 'i':
                data_column = data[column.name.upper()][row_index].data.astype(np.int16).tolist()
            elif column.name in expand_dchisq:
                j = expand_dchisq.index(column.name)
                data_column = data['DCHISQ'][row_index, j].tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Target(SchemaMixin, Base):
    """Representation of the pure-desitarget quantities in the
    ``TARGETPHOT`` table in the targetphot files.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "survey", "tileid", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)  # fiberassign
    photsys = Column(String(1), nullable=False)  # fiberassign
    subpriority = Column(DOUBLE_PRECISION, nullable=False)  # fiberassign
    obsconditions = Column(BigInteger, nullable=False)  # fiberassign
    priority_init = Column(BigInteger, nullable=False)  # fiberassign
    numobs_init = Column(BigInteger, nullable=False)  # fiberassign
    hpxpixel = Column(BigInteger, nullable=False, index=True)
    cmx_target = Column(BigInteger, nullable=False, default=0)
    desi_target = Column(BigInteger, nullable=False, default=0)
    bgs_target = Column(BigInteger, nullable=False, default=0)
    mws_target = Column(BigInteger, nullable=False, default=0)
    sv1_desi_target = Column(BigInteger, nullable=False, default=0)
    sv1_bgs_target = Column(BigInteger, nullable=False, default=0)
    sv1_mws_target = Column(BigInteger, nullable=False, default=0)
    sv2_desi_target = Column(BigInteger, nullable=False, default=0)
    sv2_bgs_target = Column(BigInteger, nullable=False, default=0)
    sv2_mws_target = Column(BigInteger, nullable=False, default=0)
    sv3_desi_target = Column(BigInteger, nullable=False, default=0)
    sv3_bgs_target = Column(BigInteger, nullable=False, default=0)
    sv3_mws_target = Column(BigInteger, nullable=False, default=0)
    scnd_target = Column(BigInteger, nullable=False, default=0)
    sv1_scnd_target = Column(BigInteger, nullable=False, default=0)
    sv2_scnd_target = Column(BigInteger, nullable=False, default=0)
    sv3_scnd_target = Column(BigInteger, nullable=False, default=0)
    survey = Column(String(7), nullable=False, index=True)
    program = Column(String(6), nullable=False, index=True)
    tileid = Column(Integer, ForeignKey('tile.tileid'), nullable=False, index=True)  # fiberassign

    photometry = relationship("Photometry", back_populates="targets")
    tile = relationship("Tile", back_populates="targets")

    def __repr__(self):
        return "Target(targetid={0.targetid:d}, tileid={0.tileid:d}, survey='{0.survey}')".format(self)

    @classmethod
    def convert(cls, data, survey=None, tileid=None, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        survey : :class:`str`, optional
            Survey name. If not set, it will be obtained from `data`.
        tileid : :class:`int`, optional
            Tile ID number. If not set, it will be obtained from `data`.
        row_index : :class:`numpy.ndarray`, optional
            Only convert the rows indexed by `row_index`. If not specified,
            convert all rows.

        Returns
        -------
        :class:`list`
            A list of ORM objects.

        Raises
        ------
        KeyError
            If `survey` or `tileid` are not set and could not be obtained from `data`.
        """
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        check_columns = {'survey': survey, 'tileid': tileid}
        for column in check_columns:
            if check_columns[column] is None:
                if column.upper() in data.colnames:
                    log.info("Obtaining '%s' from input data file.", column)
                else:
                    msg = "Could not obtain '%s' from input data file."
                    log.critical(msg, column)
                    raise KeyError(msg % (column, ))
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'id':
                if survey is None or tileid is None:
                    s = np.array([surveyid(s) for s in data['SURVEY'][row_index].tolist()], dtype=np.int64)
                    id0 = s << 32 | data['TILEID'].astype(np.int64)
                else:
                    id0 = np.array([surveyid(survey) << 32 | tileid]*len(row_index), dtype=np.int64)
                data_column = [i0 << 64 | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'].tolist())]
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Tile(SchemaMixin, Base):
    """Representation of the tiles file.

    Notes
    -----
    Most of the data that are currently in the tiles file are derivable
    from the exposures table with much greater precision::

        CREATE VIEW f5.tile AS SELECT tileid,
            -- SURVEY, FAPRGRM, FAFLAVOR?
            COUNT(*) AS nexp, SUM(exptime) AS exptime,
            MIN(tilera) AS tilera, MIN(tiledec) AS tiledec,
            SUM(efftime_etc) AS efftime_etc, SUM(efftime_spec) AS efftime_spec,
            SUM(efftime_gfa) AS efftime_gfa, MIN(goaltime) AS goaltime,
            -- OBSSTATUS?
            SUM(lrg_efftime_dark) AS lrg_efftime_dark,
            SUM(elg_efftime_dark) AS elg_efftime_dark,
            SUM(bgs_efftime_bright) AS bgs_efftime_bright,
            SUM(lya_efftime_dark) AS lya_efftime_dark,
            -- GOALTYPE?
            MIN(mintfrac) AS mintfrac, MAX(night) AS lastnight
        FROM f5.exposure GROUP BY tileid;

    However because of some unresolved discrepancies, we'll just load the
    full tiles file for now.
    """

    tileid = Column(Integer, primary_key=True, autoincrement=False)
    survey = Column(String(20), nullable=False)
    program = Column(String(6), nullable=False)
    faprgrm = Column(String(20), nullable=False)
    faflavor = Column(String(20), nullable=False)
    nexp = Column(BigInteger, nullable=False)  # In principle this could be replaced by a count of exposures
    exptime = Column(DOUBLE_PRECISION, nullable=False)
    tilera = Column(DOUBLE_PRECISION, nullable=False)  # Calib exposures don't have RA, dec
    tiledec = Column(DOUBLE_PRECISION, nullable=False)
    efftime_etc = Column(DOUBLE_PRECISION, nullable=False)
    efftime_spec = Column(DOUBLE_PRECISION, nullable=False)
    efftime_gfa = Column(DOUBLE_PRECISION, nullable=False)
    goaltime = Column(DOUBLE_PRECISION, nullable=False)
    obsstatus = Column(String(20), nullable=False)
    lrg_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    elg_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    bgs_efftime_bright = Column(DOUBLE_PRECISION, nullable=False)
    lya_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    goaltype = Column(String(20), nullable=False)  # This is probably wider than it needs to be, but it is also backward-compatible.
    mintfrac = Column(DOUBLE_PRECISION, nullable=False)
    lastnight = Column(Integer, nullable=False)  # In principle this could be replaced by MAX(night) grouped by exposures.

    exposures = relationship("Exposure", back_populates="tile")
    fiberassign = relationship("Fiberassign", back_populates="tile")
    potential = relationship("Potential", back_populates="tile")
    targets = relationship("Target", back_populates="tile")
    ztile_redshifts = relationship("Ztile", back_populates="tile")

    def __repr__(self):
        return "Tile(tileid={0.tileid:d})".format(self)

    @classmethod
    def convert(cls, data, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
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
        data_columns = list()
        for column in cls.__table__.columns:
            data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Exposure(SchemaMixin, Base):
    """Representation of the EXPOSURES HDU in the exposures file.
    """

    night = Column(Integer, nullable=False, index=True)
    expid = Column(Integer, primary_key=True, autoincrement=False)
    tileid = Column(Integer, ForeignKey('tile.tileid'), nullable=False, index=True)
    tilera = Column(DOUBLE_PRECISION, nullable=False)  # Calib exposures don't have RA, dec
    tiledec = Column(DOUBLE_PRECISION, nullable=False)
    date_obs = Column(DateTime(True), nullable=False)
    mjd = Column(DOUBLE_PRECISION, nullable=False)
    survey = Column(String(7), nullable=False)
    program = Column(String(6), nullable=False)
    faprgrm = Column(String(16), nullable=False)
    faflavor = Column(String(19), nullable=False)
    exptime = Column(DOUBLE_PRECISION, nullable=False)
    efftime_spec = Column(DOUBLE_PRECISION, nullable=False)
    goaltime = Column(DOUBLE_PRECISION, nullable=False)
    goaltype = Column(String(7), nullable=False)  # This was increased from 6 to 7 to support 'unknown' in daily specprod.
    mintfrac = Column(DOUBLE_PRECISION, nullable=False)
    airmass = Column(REAL, nullable=False)
    ebv = Column(DOUBLE_PRECISION, nullable=False)
    seeing_etc = Column(DOUBLE_PRECISION, nullable=False)
    efftime_etc = Column(REAL, nullable=False)
    tsnr2_elg = Column(REAL, nullable=False)
    tsnr2_qso = Column(REAL, nullable=False)
    tsnr2_lrg = Column(REAL, nullable=False)
    tsnr2_lya = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_bgs = Column(REAL, nullable=False)
    tsnr2_gpbdark = Column(REAL, nullable=False)
    tsnr2_gpbbright = Column(REAL, nullable=False)
    tsnr2_gpbbackup = Column(REAL, nullable=False)
    lrg_efftime_dark = Column(REAL, nullable=False)
    elg_efftime_dark = Column(REAL, nullable=False)
    bgs_efftime_bright = Column(REAL, nullable=False)
    lya_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    gpb_efftime_dark = Column(REAL, nullable=False)
    gpb_efftime_bright = Column(REAL, nullable=False)
    gpb_efftime_backup = Column(REAL, nullable=False)
    transparency_gfa = Column(DOUBLE_PRECISION, nullable=False)
    seeing_gfa = Column(DOUBLE_PRECISION, nullable=False)
    fiber_fracflux_gfa = Column(DOUBLE_PRECISION, nullable=False)
    fiber_fracflux_elg_gfa = Column(DOUBLE_PRECISION, nullable=False)
    fiber_fracflux_bgs_gfa = Column(DOUBLE_PRECISION, nullable=False)
    fiberfac_gfa = Column(DOUBLE_PRECISION, nullable=False)
    fiberfac_elg_gfa = Column(DOUBLE_PRECISION, nullable=False)
    fiberfac_bgs_gfa = Column(DOUBLE_PRECISION, nullable=False)
    airmass_gfa = Column(DOUBLE_PRECISION, nullable=False)
    sky_mag_ab_gfa = Column(DOUBLE_PRECISION, nullable=False)
    sky_mag_g_spec = Column(DOUBLE_PRECISION, nullable=False)
    sky_mag_r_spec = Column(DOUBLE_PRECISION, nullable=False)
    sky_mag_z_spec = Column(DOUBLE_PRECISION, nullable=False)
    efftime_gfa = Column(DOUBLE_PRECISION, nullable=False)
    efftime_dark_gfa = Column(DOUBLE_PRECISION, nullable=False)
    efftime_bright_gfa = Column(DOUBLE_PRECISION, nullable=False)
    efftime_backup_gfa = Column(DOUBLE_PRECISION, nullable=False)

    tile = relationship("Tile", back_populates="exposures")
    frames = relationship("Frame", back_populates="exposure")

    def __repr__(self):
        return "Exposure(night={0.night:d}, expid={0.expid:d}, tileid={0.tileid:d})".format(self)

    @classmethod
    def convert(cls, data, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
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
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'date_obs':
                data_column = list(map(utc.localize, Time(data[row_index]['MJD'], format='mjd').to_value('datetime').tolist()))
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Frame(SchemaMixin, Base):
    """Representation of the FRAMES HDU in the exposures file.

    Notes
    -----
    The column ``frameid`` is a combination of ``expid`` and the camera name::

        frameid = 100*expid + cameraid(camera)

    where ``cameraid()`` is :func:`specprodDB.util.cameraid`.
    """

    frameid = Column(Integer, primary_key=True, autoincrement=False)  # Arbitrary integer composed from expid + cameraid
    # frameid = Column(BigInteger, primary_key=True, autoincrement=True)
    night = Column(Integer, nullable=False, index=True)
    expid = Column(Integer, ForeignKey('exposure.expid'), nullable=False)
    tileid = Column(Integer, nullable=False, index=True)
    #  4 TILERA               D
    #  5 TILEDEC              D
    #  6 MJD                  D
    mjd = Column(DOUBLE_PRECISION, nullable=False)
    #  7 EXPTIME              E
    exptime = Column(REAL, nullable=False)
    #  8 AIRMASS              E
    #  9 EBV                  E
    ebv = Column(REAL, nullable=False)
    # 10 SEEING_ETC           D
    # 11 EFFTIME_ETC          E
    # 12 CAMERA               2A
    camera = Column(String(2), nullable=False)
    # 13 TSNR2_GPBDARK        E
    # 14 TSNR2_ELG            E
    # 15 TSNR2_GPBBRIGHT      E
    # 16 TSNR2_LYA            D
    # 17 TSNR2_BGS            E
    # 18 TSNR2_GPBBACKUP      E
    # 19 TSNR2_QSO            E
    # 20 TSNR2_LRG            E
    tsnr2_gpbdark = Column(REAL, nullable=False)
    tsnr2_elg = Column(REAL, nullable=False)
    tsnr2_gpbbright = Column(REAL, nullable=False)
    tsnr2_lya = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_bgs = Column(REAL, nullable=False)
    tsnr2_gpbbackup = Column(REAL, nullable=False)
    tsnr2_qso = Column(REAL, nullable=False)
    tsnr2_lrg = Column(REAL, nullable=False)
    # 21 SURVEY               7A
    # 22 GOALTYPE             6A
    # 23 FAPRGRM              15A
    # 24 FAFLAVOR             18A
    # 25 MINTFRAC             D
    # 26 GOALTIME             D

    exposure = relationship("Exposure", back_populates="frames")

    def __repr__(self):
        return "Frame(expid={0.expid:d}, camera='{0.camera}')".format(self)

    @classmethod
    def convert(cls, data, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
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
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'frameid':
                data_column = (100*data[row_index]['EXPID'] + np.array([cameraid(c) for c in data[row_index]['CAMERA']],
                                                                       dtype=data[row_index]['EXPID'].dtype)).tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Fiberassign(SchemaMixin, Base):
    """Representation of the FIBERASSIGN table in a fiberassign file.

    Notes
    -----
    * Targets are assigned to a ``location``.  A ``location`` happens to
      correspond to a ``fiber``, but this correspondence could change over
      time, and therefore should not be assumed to be a rigid 1:1 mapping.
    * ``PLATE_RA``, ``PLATE_DEC`` are sometimes missing.  These can be
      copies of ``TARGET_RA``, ``TARGET_DEC``, but in principle they could
      be different if chromatic offsets in targeting positions were
      ever implemented.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "tileid", "targetid", "location", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    tileid = Column(Integer, ForeignKey('tile.tileid'), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
    petal_loc = Column(SmallInteger, nullable=False)
    device_loc = Column(Integer, nullable=False)
    location = Column(Integer, nullable=False, index=True)
    fiber = Column(Integer, nullable=False)
    fiberstatus = Column(Integer, nullable=False)
    target_ra = Column(DOUBLE_PRECISION, nullable=False)
    target_dec = Column(DOUBLE_PRECISION, nullable=False)
    pmra = Column(REAL, nullable=False)
    pmdec = Column(REAL, nullable=False)
    ref_epoch = Column(REAL, nullable=False)
    lambda_ref = Column(REAL, nullable=False)
    fa_target = Column(BigInteger, nullable=False)
    fa_type = Column(SmallInteger, nullable=False)
    fiberassign_x = Column(REAL, nullable=False)
    fiberassign_y = Column(REAL, nullable=False)
    priority = Column(Integer, nullable=False)
    subpriority = Column(DOUBLE_PRECISION, nullable=False)
    parallax = Column(REAL, nullable=False)
    # plate_ra = Column(DOUBLE_PRECISION, nullable=False)
    # plate_dec = Column(DOUBLE_PRECISION, nullable=False)

    photometry = relationship("Photometry", back_populates="fiberassign")
    tile = relationship("Tile", back_populates="fiberassign")

    def __repr__(self):
        return "Fiberassign(tileid={0.tileid:d}, targetid={0.targetid:d}, location={0.location:d})".format(self)

    @classmethod
    def convert(cls, data, tileid=None, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        tileid : :class:`int`, optional
            Tile ID number. If not set, it will be obtained from `data`.
        row_index : :class:`numpy.ndarray`, optional
            Only convert the rows indexed by `row_index`. If not specified,
            convert all rows.

        Returns
        -------
        :class:`list`
            A list of ORM objects.

        Raises
        ------
        KeyError
            If `tileid` is not set and could not be obtained from `data`.
        """
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        if tileid is None:
            try:
                tileid = data.meta['TILEID']
            except KeyError:
                log.critical("Could not obtain 'TILEID' from metadata!")
                raise
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'id':
                id0 = (data['LOCATION'][row_index].base.astype(np.int64) << 32) | tileid
                data_column = [(i0 << 64) | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'][row_index].tolist())]
            elif column.name == 'tileid':
                data_column = [tileid]*len(row_index)
            elif column.name == 'plate_ra' and 'PLATE_RA' not in data.colnames:
                # This will usually be ignored, because plate_ra is not necessarily a database column.
                data_column = data['TARGET_RA'][row_index].tolist()
            elif column.name == 'plate_dec' and 'PLATE_DEC' not in data.colnames:
                # This will usually be ignored, because plate_dec is not necessarily a database column.
                data_column = data['TARGET_DEC'][row_index].tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Potential(SchemaMixin, Base):
    """Representation of the POTENTIAL_ASSIGNMENTS table in a fiberassign file.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "tileid", "targetid", "location", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    tileid = Column(Integer, ForeignKey('tile.tileid'), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
    fiber = Column(Integer, nullable=False)
    location = Column(Integer, nullable=False, index=True)

    photometry = relationship("Photometry", back_populates="potential")
    tile = relationship("Tile", back_populates="potential")

    def __repr__(self):
        return "Potential(tileid={0.tileid:d}, targetid={0.targetid:d}, location={0.location:d})".format(self)

    @classmethod
    def convert(cls, data, tileid=None, row_index=None):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        tileid : :class:`int`, optional
            Tile ID number. If not set, it will be obtained from `data`.
        row_index : :class:`numpy.ndarray`, optional
            Only convert the rows indexed by `row_index`. If not specified,
            convert all rows.

        Returns
        -------
        :class:`list`
            A list of ORM objects.

        Raises
        ------
        KeyError
            If `tileid` is not set and could not be obtained from `data`.
        """
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        #
        # Potential table has no floating point columns.
        #
        # data = finitize(data)
        if tileid is None:
            try:
                tileid = data.meta['TILEID']
            except KeyError:
                log.critical("Could not obtain 'TILEID' from metadata!")
                raise
        data_columns = list()
        for column in cls.__table__.columns:
            if column.name == 'id':
                id0 = (data['LOCATION'][row_index].base.astype(np.int64) << 32) | tileid
                data_column = [(i0 << 64) | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'][row_index].tolist())]
            elif column.name == 'tileid':
                data_column = [tileid]*len(row_index)
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Zpix(SchemaMixin, Base):
    """Representation of the ``ZCATALOG`` table in zpix files.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "survey", "program", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
    desiname = Column(String(22), nullable=False, index=True)
    survey = Column(String(7), nullable=False, index=True)
    program = Column(String(6), nullable=False, index=True)
    spgrp = Column(String(10), nullable=False, index=True)
    spgrpval = Column(Integer, nullable=False, index=True)
    healpix = Column(Integer, nullable=False, index=True)
    z = Column(DOUBLE_PRECISION, index=True, nullable=False)
    zerr = Column(DOUBLE_PRECISION, nullable=False)
    zwarn = Column(BigInteger, index=True, nullable=False)
    chi2 = Column(DOUBLE_PRECISION, nullable=False)
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
    npixels = Column(BigInteger, nullable=False)
    spectype = Column(String(6), index=True, nullable=False)
    subtype = Column(String(20), index=True, nullable=False)
    ncoeff = Column(BigInteger, nullable=False)
    deltachi2 = Column(DOUBLE_PRECISION, nullable=False)
    coadd_fiberstatus = Column(Integer, nullable=False)
    #
    # Skipping columns that are in other tables.
    #
    # These target bitmask columns are *not* the same as
    # in the zall-pix-specprod.fits file. They will be replaced
    # after the fact with values from the bitwise-or of
    # values in the target table.
    #
    cmx_target = Column(BigInteger, nullable=False, default=0)
    desi_target = Column(BigInteger, nullable=False, default=0)
    bgs_target = Column(BigInteger, nullable=False, default=0)
    mws_target = Column(BigInteger, nullable=False, default=0)
    scnd_target = Column(BigInteger, nullable=False, default=0)
    sv1_desi_target = Column(BigInteger, nullable=False, default=0)
    sv1_bgs_target = Column(BigInteger, nullable=False, default=0)
    sv1_mws_target = Column(BigInteger, nullable=False, default=0)
    sv1_scnd_target = Column(BigInteger, nullable=False, default=0)
    sv2_desi_target = Column(BigInteger, nullable=False, default=0)
    sv2_bgs_target = Column(BigInteger, nullable=False, default=0)
    sv2_mws_target = Column(BigInteger, nullable=False, default=0)
    sv2_scnd_target = Column(BigInteger, nullable=False, default=0)
    sv3_desi_target = Column(BigInteger, nullable=False, default=0)
    sv3_bgs_target = Column(BigInteger, nullable=False, default=0)
    sv3_mws_target = Column(BigInteger, nullable=False, default=0)
    sv3_scnd_target = Column(BigInteger, nullable=False, default=0)
    #
    # Skipping columns that are in other tables.
    #
    coadd_numexp = Column(SmallInteger, nullable=False)
    coadd_exptime = Column(REAL, nullable=False)
    coadd_numnight = Column(SmallInteger, nullable=False)
    coadd_numtile = Column(SmallInteger, nullable=False)
    mean_delta_x = Column(REAL, nullable=False)
    rms_delta_x = Column(REAL, nullable=False)
    mean_delta_y = Column(REAL, nullable=False)
    rms_delta_y = Column(REAL, nullable=False)
    mean_fiber_ra = Column(DOUBLE_PRECISION, nullable=False)
    std_fiber_ra = Column(REAL, nullable=False)
    mean_fiber_dec = Column(DOUBLE_PRECISION, nullable=False)
    std_fiber_dec = Column(REAL, nullable=False)
    mean_psf_to_fiber_specflux = Column(REAL, nullable=False)
    tsnr2_gpbdark_b = Column(REAL, nullable=False)
    tsnr2_elg_b = Column(REAL, nullable=False)
    tsnr2_gpbbright_b = Column(REAL, nullable=False)
    tsnr2_lya_b = Column(REAL, nullable=False)
    tsnr2_bgs_b = Column(REAL, nullable=False)
    tsnr2_gpbbackup_b = Column(REAL, nullable=False)
    tsnr2_qso_b = Column(REAL, nullable=False)
    tsnr2_lrg_b = Column(REAL, nullable=False)
    tsnr2_gpbdark_r = Column(REAL, nullable=False)
    tsnr2_elg_r = Column(REAL, nullable=False)
    tsnr2_gpbbright_r = Column(REAL, nullable=False)
    tsnr2_lya_r = Column(REAL, nullable=False)
    tsnr2_bgs_r = Column(REAL, nullable=False)
    tsnr2_gpbbackup_r = Column(REAL, nullable=False)
    tsnr2_qso_r = Column(REAL, nullable=False)
    tsnr2_lrg_r = Column(REAL, nullable=False)
    tsnr2_gpbdark_z = Column(REAL, nullable=False)
    tsnr2_elg_z = Column(REAL, nullable=False)
    tsnr2_gpbbright_z = Column(REAL, nullable=False)
    tsnr2_lya_z = Column(REAL, nullable=False)
    tsnr2_bgs_z = Column(REAL, nullable=False)
    tsnr2_gpbbackup_z = Column(REAL, nullable=False)
    tsnr2_qso_z = Column(REAL, nullable=False)
    tsnr2_lrg_z = Column(REAL, nullable=False)
    tsnr2_gpbdark = Column(REAL, nullable=False)
    tsnr2_elg = Column(REAL, nullable=False)
    tsnr2_gpbbright = Column(REAL, nullable=False)
    tsnr2_lya = Column(REAL, nullable=False)
    tsnr2_bgs = Column(REAL, nullable=False)
    tsnr2_gpbbackup = Column(REAL, nullable=False)
    tsnr2_qso = Column(REAL, nullable=False)
    tsnr2_lrg = Column(REAL, nullable=False)
    sv_nspec = Column(SmallInteger, nullable=False)
    sv_primary = Column(Boolean, nullable=False)
    main_nspec = Column(SmallInteger, nullable=False)
    main_primary = Column(Boolean, nullable=False)
    zcat_nspec = Column(SmallInteger, nullable=False)
    zcat_primary = Column(Boolean, nullable=False)
    # firstnight = Column(Integer, nullable=False)
    # lastnight = Column(Integer, nullable=False)
    min_mjd = Column(DOUBLE_PRECISION, nullable=False)
    mean_mjd = Column(DOUBLE_PRECISION, nullable=False)
    max_mjd = Column(DOUBLE_PRECISION, nullable=False)

    photometry = relationship("Photometry", back_populates="zpix_redshifts")

    def __repr__(self):
        return "Zpix(targetid={0.targetid:d}, survey='{0.survey}', program='{0.program}')".format(self)

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
        spgrp : :class:`str`, optional
            Normally this will be set to the default value: 'cumulative'.

        Returns
        -------
        :class:`list`
            A list of ORM objects.

        Raises
        ------
        KeyError
            If `survey` or `program` are not set and the equivalent data
            are not available in `data`.
        """
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        default_columns = {'spgrp': 'healpix',
                           'sv_nspec': 0, 'main_nspec': 0, 'zcat_nspec': 0,
                           'sv_primary': False, 'main_primary': False, 'zcat_primary': False}
        #
        # Reductions like guadalupe may not have the full set of target bitmasks
        #
        surveys = ('', 'sv1', 'sv2', 'sv3')
        programs = ('desi', 'bgs', 'mws', 'scnd')
        masks = ['cmx_target'] + [('_'.join(p) if p[0] else p[1]) + '_target'
                                  for p in itertools.product(surveys, programs)]
        for mask in masks:
            default_columns[mask] = 0
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
            elif column.name == 'desiname':
                data_column = radec_to_desiname(data['TARGET_RA'][row_index], data['TARGET_DEC'][row_index]).tolist()
            elif column.name == 'spgrpval':
                data_column = data['HEALPIX'][row_index].tolist()
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
            elif column.name.startswith('coeff_'):
                coeff_index = int(column.name.split('_')[1])
                data_column = data['COEFF'][row_index, coeff_index].tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


class Ztile(SchemaMixin, Base):
    """Representation of the ``ZCATALOG`` table in ztile files.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "spgrp", "spgrpval", "tileid", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetphotid = Column(Numeric(39), ForeignKey("target.id"), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
    desiname = Column(String(22), nullable=False, index=True)
    survey = Column(String(7), nullable=False, index=True)
    program = Column(String(6), nullable=False, index=True)
    spgrp = Column(String, nullable=False, index=True)
    spgrpval = Column(Integer, nullable=False, index=True)
    z = Column(DOUBLE_PRECISION, index=True, nullable=False)
    zerr = Column(DOUBLE_PRECISION, nullable=False)
    zwarn = Column(BigInteger, index=True, nullable=False)
    chi2 = Column(DOUBLE_PRECISION, nullable=False)
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
    npixels = Column(BigInteger, nullable=False)
    spectype = Column(String(6), index=True, nullable=False)
    subtype = Column(String(20), index=True, nullable=False)
    ncoeff = Column(BigInteger, nullable=False)
    deltachi2 = Column(DOUBLE_PRECISION, nullable=False)
    coadd_fiberstatus = Column(Integer, nullable=False)
    #
    # Skipping columns that are in other tables.
    #
    tileid = Column(Integer, ForeignKey("tile.tileid"), nullable=False, index=True)
    coadd_numexp = Column(SmallInteger, nullable=False)
    coadd_exptime = Column(REAL, nullable=False)
    coadd_numnight = Column(SmallInteger, nullable=False)
    coadd_numtile = Column(SmallInteger, nullable=False)
    mean_delta_x = Column(REAL, nullable=False)
    rms_delta_x = Column(REAL, nullable=False)
    mean_delta_y = Column(REAL, nullable=False)
    rms_delta_y = Column(REAL, nullable=False)
    mean_fiber_ra = Column(DOUBLE_PRECISION, nullable=False)
    std_fiber_ra = Column(REAL, nullable=False)
    mean_fiber_dec = Column(DOUBLE_PRECISION, nullable=False)
    std_fiber_dec = Column(REAL, nullable=False)
    mean_psf_to_fiber_specflux = Column(REAL, nullable=False)
    mean_fiber_x = Column(REAL, nullable=False)
    mean_fiber_y = Column(REAL, nullable=False)
    tsnr2_gpbdark_b = Column(REAL, nullable=False)
    tsnr2_elg_b = Column(REAL, nullable=False)
    tsnr2_gpbbright_b = Column(REAL, nullable=False)
    tsnr2_lya_b = Column(REAL, nullable=False)
    tsnr2_bgs_b = Column(REAL, nullable=False)
    tsnr2_gpbbackup_b = Column(REAL, nullable=False)
    tsnr2_qso_b = Column(REAL, nullable=False)
    tsnr2_lrg_b = Column(REAL, nullable=False)
    tsnr2_gpbdark_r = Column(REAL, nullable=False)
    tsnr2_elg_r = Column(REAL, nullable=False)
    tsnr2_gpbbright_r = Column(REAL, nullable=False)
    tsnr2_lya_r = Column(REAL, nullable=False)
    tsnr2_bgs_r = Column(REAL, nullable=False)
    tsnr2_gpbbackup_r = Column(REAL, nullable=False)
    tsnr2_qso_r = Column(REAL, nullable=False)
    tsnr2_lrg_r = Column(REAL, nullable=False)
    tsnr2_gpbdark_z = Column(REAL, nullable=False)
    tsnr2_elg_z = Column(REAL, nullable=False)
    tsnr2_gpbbright_z = Column(REAL, nullable=False)
    tsnr2_lya_z = Column(REAL, nullable=False)
    tsnr2_bgs_z = Column(REAL, nullable=False)
    tsnr2_gpbbackup_z = Column(REAL, nullable=False)
    tsnr2_qso_z = Column(REAL, nullable=False)
    tsnr2_lrg_z = Column(REAL, nullable=False)
    tsnr2_gpbdark = Column(REAL, nullable=False)
    tsnr2_elg = Column(REAL, nullable=False)
    tsnr2_gpbbright = Column(REAL, nullable=False)
    tsnr2_lya = Column(REAL, nullable=False)
    tsnr2_bgs = Column(REAL, nullable=False)
    tsnr2_gpbbackup = Column(REAL, nullable=False)
    tsnr2_qso = Column(REAL, nullable=False)
    tsnr2_lrg = Column(REAL, nullable=False)
    sv_nspec = Column(SmallInteger, nullable=False)
    sv_primary = Column(Boolean, nullable=False)
    main_nspec = Column(SmallInteger, nullable=False)
    main_primary = Column(Boolean, nullable=False)
    zcat_nspec = Column(SmallInteger, nullable=False)
    zcat_primary = Column(Boolean, nullable=False)
    firstnight = Column(Integer, nullable=False)
    lastnight = Column(Integer, nullable=False)
    min_mjd = Column(DOUBLE_PRECISION, nullable=False)
    mean_mjd = Column(DOUBLE_PRECISION, nullable=False)
    max_mjd = Column(DOUBLE_PRECISION, nullable=False)

    photometry = relationship("Photometry", back_populates="ztile_redshifts")
    tile = relationship("Tile", back_populates="ztile_redshifts")

    def __repr__(self):
        return "Ztile(targetid={0.targetid:d}, tileid={0.tileid:d}, spgrp='{0.spgrp}', spgrpval={0.spgrpval:d})".format(self)

    @classmethod
    def convert(cls, data, survey=None, program=None, tileid=None, night=None,
                row_index=None, spgrp='cumulative'):
        """Convert `data` into ORM objects ready for loading.

        Parameters
        ----------
        data : :class:`~astropy.table.Table`
            Data table to convert.
        survey : :class:`str`, optional
            Survey name. If not set, it will be obtained from `data`.
        program : :class:`str`, optional
            Program name. If not set, it will be obtained from `data`.
        tileid : :class:`int`, optional
            Tile ID number. If not set, it will be obtained from `data`.
        night : :class:`int`, optional
            Night number. This is loaded into the ``firstnight`` column.
            If not set, it will be obtained from `data`.
        row_index : :class:`numpy.ndarray`, optional
            Only convert the rows indexed by `row_index`. If not specified,
            convert all rows.
        spgrp : :class:`str`, optional
            Normally this will be set to the default value: 'cumulative'.

        Returns
        -------
        :class:`list`
            A list of ORM objects.

        Raises
        ------
        KeyError
            If `survey`, `program`, `tileid` or `night` are not set and the
            equivalent data are not available in `data`.

        Notes
        -----
        * If `tileid` is set, this method assumes `data` comes from one and only one
          tile.
        * The above has a secondary assumption that, at least for cumulative
          tile-based spectra, the first night is the same for all spectra.
        * `night` becomes ``firstnight``, while ``spgrpval`` is equivalent to
          "lastnight" for cumulative tile-based spectra.
        """
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        default_columns = {'spgrp': spgrp,
                           'sv_nspec': 0, 'main_nspec': 0, 'zcat_nspec': 0,
                           'sv_primary': False, 'main_primary': False, 'zcat_primary': False}
        check_columns = {'survey': survey, 'program': program,
                         'tileid': tileid, 'firstnight': night}
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
                id0 = spgrpid(spgrp) << 27 | data['SPGRPVAL'][row_index].base.astype(np.int64)
                data_column = [(i0 << 64) | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'][row_index].tolist())]
            elif column.name == 'targetphotid':
                if 'survey' in default_columns:
                    id0 = np.array([surveyid(survey) << 32 | tileid]*len(row_index), dtype=np.int64)
                else:
                    s = np.array([surveyid(s) for s in data['SURVEY'][row_index].tolist()], dtype=np.int64)
                    id0 = s << 32 | data['TILEID'][row_index].astype(np.int64)
                data_column = [(i0 << 64) | i1 for i0, i1 in zip(id0.tolist(), data['TARGETID'][row_index].tolist())]
            elif column.name == 'desiname':
                data_column = radec_to_desiname(data['TARGET_RA'][row_index], data['TARGET_DEC'][row_index]).tolist()
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
            elif column.name.startswith('coeff_'):
                coeff_index = int(column.name.split('_')[1])
                data_column = data['COEFF'][row_index, coeff_index].tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]


def upsert(rows, do_nothing=False):
    """Convert a list of ORM objects into an ``INSERT ... ON CONFLICT`` statement.

    Parameters
    ----------
    rows : :class:`list`
        A list of ORM objects. All items should be the same type.
    do_nothing : :class:`bool`, optional
        If ``True``, *do not* attempt to update existing rows.

    Returns
    -------
    :class:`~sqlalchemy.dialects.postgresql.Insert`
        A specialzed INSERT statement ready for execution.
    """
    cls = rows[0].__class__
    pk = [c for c in cls.__table__.columns if c.primary_key][0]
    inserts = list()
    for row in rows:
        rr = row.__dict__.copy()
        del rr['_sa_instance_state']
        inserts.append(rr)
    stmt = pg_insert(cls).values(inserts)
    if do_nothing:
        stmt = stmt.on_conflict_do_nothing(index_elements=[getattr(cls, pk.name)])
    else:
        stmt = stmt.on_conflict_do_update(index_elements=[getattr(cls, pk.name)],
                                          set_=dict([(c, getattr(stmt.excluded, c.name))
                                                     for c in cls.__table__.columns if c.name != pk.name]))
    return stmt


def deduplicate_targetid(data):
    """Find targetphot rows that are not already loaded into the Photometry
    table *and* resolve any duplicate TARGETID.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`numpy.ndarray`
        An array of rows that are safe to load.
    """
    rows = dbSession.query(Photometry.targetid, Photometry.ls_id).order_by(Photometry.targetid).all()
    loaded_targetid = Table()
    loaded_targetid['TARGETID'] = np.array([r[0] for r in rows])
    loaded_targetid['LS_ID'] = np.array([r[1] for r in rows])
    #
    # Find TARGETIDs that do not exist in Photometry
    #
    j = join(data['TARGETID', 'RELEASE'], loaded_targetid, join_type='left', keys='TARGETID')
    load_rows = np.zeros((len(data),), dtype=bool)
    try:
        load_targetids = j['TARGETID'][j['LS_ID'].mask]
    except AttributeError:
        #
        # This means *every* TARGETID is already loaded.
        #
        pass
    else:
        unique_targetid, targetid_index = np.unique(data['TARGETID'].data, return_index=True)
        for t in load_targetids:
            load_rows[targetid_index[unique_targetid == t]] = True
    return load_rows


def finitize(data, replacement_value=-9999.0):
    """Convert ``NaN`` and other non-finite floating point values.

    Parameters
    ----------
    data : :class:`~astropy.table.Table`
        Data table to convert.
    replacement_value : :class:`float`, optional
        Replace ``NaN`` or other non-finite values with this value (default -9999.0).

    Returns
    -------
    :class:`~astropy.table.Table`
        The input `data` modified in-place.
    """
    try:
        colnames = data.names
    except AttributeError:
        colnames = data.colnames
    masked = dict()
    for col in colnames:
        if data[col].dtype.kind == 'f':
            if isinstance(data[col], MaskedColumn):
                bad = ~np.isfinite(data[col].data.data)
                masked[col] = True
            else:
                bad = ~np.isfinite(data[col])
            if np.any(bad):
                if bad.ndim == 1:
                    log.warning("%d rows of bad data detected in column " +
                                "%s.", bad.sum(), col)
                elif bad.ndim == 2:
                    nbadrows = len(bad.sum(1).nonzero()[0])
                    nbaditems = bad.sum(1).sum()
                    log.warning("%d rows (%d items) of bad data detected in column " +
                                "%s.", nbadrows, nbaditems, col)
                else:
                    log.warning("Bad data detected in high-dimensional column %s.", col)
                if col in masked:
                    log.debug("data['%s'].data.data[bad] = %f", col, replacement_value)
                    log.debug("data['%s'].mask[bad] = False", col)
                    data[col].data.data[bad] = replacement_value
                    data[col].mask[bad] = False
                else:
                    log.debug("data['%s'][bad] = %f", col, replacement_value)
                    data[col][bad] = replacement_value
    return data


def load_file(filepaths, tcls, hdu=1, row_filter=None, q3c=None, chunksize=50000):
    """Load data file into the database, assuming that column names map
    to database column names with no surprises.

    Parameters
    ----------
    filepaths : :class:`str` or :class:`list`
        Full path to the data file or set of data files.
    tcls : :class:`sqlalchemy.ext.declarative.api.DeclarativeMeta`
        The table to load, represented by its class.
    hdu : :class:`int` or :class:`str`, optional
        Read a data table from this HDU (default 1).
    row_filter : callable, optional
        If set, apply this filter to the rows to be loaded.  The function
        should return :class:`bool`, with ``True`` meaning a good row.
    q3c : :class:`str`, optional
        If set, create q3c index on the table, using the RA column
        named `q3c`.
    chunksize : :class:`int`, optional
        If set, load database `chunksize` rows at a time (default 50000).

    Returns
    -------
    :class:`int`
        The grand total of rows loaded.
    """
    tn = tcls.__tablename__
    if isinstance(filepaths, str):
        filepaths = [filepaths]
    log.info("Identified %d files for ingestion.", len(filepaths))
    loaded_rows = 0
    for filepath in filepaths:
        if filepath.endswith('.fits') or filepath.endswith('.fits.gz'):
            data = Table.read(filepath, hdu=hdu, format='fits')
            log.info("Read %d rows of data from %s HDU %s.", len(data), filepath, hdu)
        elif filepath.endswith('.ecsv'):
            data = Table.read(filepath, format='ascii.ecsv')
            log.info("Read %d rows of data from %s.", len(data), filepath)
        elif filepath.endswith('.csv'):
            data = Table.read(filepath, format='ascii.csv')
            log.info("Read %d rows of data from %s.", len(data), filepath)
        else:
            log.error("Unrecognized data file, %s!", filepath)
            return
        if row_filter is None:
            good_rows = np.ones((len(data),), dtype=bool)
        else:
            good_rows = row_filter(data)
        if good_rows.sum() == 0:
            log.info("Row filter removed all data rows, skipping %s.", filepath)
            continue
        log.info("Row filter applied on %s; %d rows remain.", tn, good_rows.sum())
        orm_objects = tcls.convert(data, row_index=good_rows)
        log.info("Converted data to ORM objects on %s.", tn)
        del data
        finalrows = len(orm_objects)
        n_chunks = finalrows//chunksize
        if finalrows % chunksize:
            n_chunks += 1
        for k in range(n_chunks):
            data_chunk = orm_objects[k*chunksize:(k+1)*chunksize]
            if len(data_chunk) > 0:
                loaded_rows += len(data_chunk)
                dbSession.add_all(data_chunk)
                dbSession.commit()
                log.info("Inserted %d rows in %s.",
                         min((k+1)*chunksize, finalrows), tn)
            else:
                log.error("Detected empty data chunk in %s!", tn)
    if q3c is not None:
        q3c_index(tn, ra=q3c)
    return loaded_rows


def q3c_index(table, ra='ra'):
    """Create a q3c index on a table.

    Parameters
    ----------
    table : :class:`str`
        Name of the table to index.
    ra : :class:`str`, optional
        If the RA, Dec columns are called something besides "ra" and "dec",
        set its name.  For example, ``ra='target_ra'``.
    """
    q3c_sql = """CREATE INDEX IF NOT EXISTS ix_{table}_q3c_ang2ipix ON {schema}.{table} (q3c_ang2ipix({ra}, {dec}));
    CLUSTER {schema}.{table} USING ix_{table}_q3c_ang2ipix;
    ANALYZE {schema}.{table};
    """.format(ra=ra, dec=ra.lower().replace('ra', 'dec'),
               schema=schemaname, table=table)
    log.info("Creating q3c index on %s.%s.", schemaname, table)
    dbSession.execute(text(q3c_sql))
    log.info("Finished q3c index on %s.%s.", schemaname, table)
    dbSession.commit()
    return


def load_versions(photometry, redshift, release, specprod, tiles):
    """Load version metadata.

    The inputs to this function are normally specified in the specprod
    configuration file. Other necessary metadata are obtained at import time.

    Parameters
    ----------
    photometry : :class:`str`
        Photometry catalog.
    redshift : :class:`str`
        Redshift catalog version.
    release : :class:`str`
        Data release, *e.g.* 'edr', 'dr1'.
    specprod : :class:`str`
        The specprod version. Usually, but not always, the same as the schema name.
    tiles : :class:`str`
        The tiles (fiberassign file) version.
    """
    log.info("Loading version metadata.")
    version_table = Table()
    version_table['PACKAGE'] = np.array(['astropy', 'desiutil', 'lsd9-photometry',
                                         'numpy', 'redshift', 'release', 'specprod',
                                         'specprod-db', 'sqlalchemy', 'tiles'])
    version_table['VERSION'] = np.array([astropy_version, desiutil_version, photometry,
                                         np.__version__, redshift, release, specprod,
                                         specprodDB_version, sqlalchemy_version, tiles])
    versions = Version.convert(version_table)
    dbSession.add_all(versions)
    dbSession.commit()
    log.info("Completed loading version metadata.")
    return


def setup_db(dbfile='specprod.db', hostname=None, username='desi_admin',
             schema=None, overwrite=False, public=False, verbose=False):
    """Initialize the database connection.

    Parameters
    ----------
    dbfile : :class:`str`, optional
        Name of a SQLite file for output (default ``specprod.db``).
        If no path is specified in the file name, the current working
        directory will be used.
    hostname : :class:`str`, optional
        Name of a PostgreSQL server for output.
    username : :class:`str`, optional
        Username on a PostgreSQL server for database connection.
    schema : :class:`str`, optional
        Name of database schema that will contain output tables.
    overwrite : :class:`bool`, optional
        If ``True``, overwrite any existing schema or table.
    public : :class:`bool`, optional
        If ``True``, allow public access to the database or schema.
    verbose : :class:`bool`, optional
        If ``True``, Print extra debugging information for SQL queries.

    Returns
    -------
    :class:`bool`
        ``True`` if the configured database is a PostgreSQL database.

    Raises
    ------
    RuntimeError
        If database connection details could not be found.
    """
    global engine, schemaname
    #
    # Schema creation
    #
    if schema:
        schemaname = schema
        # event.listen(Base.metadata, 'before_create', CreateSchema(schemaname))
        # if overwrite:
        #     event.listen(Base.metadata, 'before_create',
        #                  DDL('DROP SCHEMA IF EXISTS {0} CASCADE'.format(schemaname)))
        event.listen(Base.metadata, 'before_create',
                     DDL(f'CREATE SCHEMA IF NOT EXISTS {schema};'))
        grant = f"""GRANT USAGE ON SCHEMA {schema} TO desi;
GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO desi;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO desi;
"""
        if public:
            grant += f"""GRANT USAGE ON SCHEMA {schema} TO desi_public;
GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO desi_public;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO desi_public;
"""
        event.listen(Base.metadata, 'after_create', DDL(grant))
    #
    # Create the file.
    #
    if hostname:
        db_connection = parse_pgpass(hostname=hostname,
                                     username=username)
        if db_connection is None:
            log.critical("Could not load database information!")
            raise RuntimeError("Could not load database information!")
    else:
        if os.path.basename(dbfile) == dbfile:
            db_file = os.path.join(os.path.abspath('.'), dbfile)
        else:
            db_file = dbfile
        if overwrite and os.path.exists(db_file):
            log.info("Removing file: %s.", db_file)
            os.remove(db_file)
        db_connection = 'sqlite://'+db_file
    #
    # SQLAlchemy stuff.
    #
    engine = create_engine(db_connection, echo=verbose)
    dbSession.remove()
    dbSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
    for tab in Base.metadata.tables.values():
        tab.schema = schemaname
    if overwrite:
        log.info("Begin creating tables.")
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        log.info("Finished creating tables.")
    return hostname is not None


def get_options(description="Load redshift data into a specprod database."):
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
    prsr.add_argument('-l', '--load', action='store', dest='load',
                      default='exposures', metavar='STAGE',
                      help='Load the set of files associated with STAGE (default "%(default)s").')
    prsr.add_argument('datapath', metavar='DIR', help='Load the data in DIR.')
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
    freeze_iers()
    #
    # command-line arguments
    #
    options = get_options()
    #
    # Logging
    #
    if options.verbose:
        log = get_logger(DEBUG, timestamp=True)
    else:
        log = get_logger(INFO, timestamp=True)
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
    postgresql = setup_db(hostname=config[specprod]['hostname'],
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
    if target_summary:
        target_files = os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', f'targetphot-potential-{specprod}.fits')
    else:
        target_files = glob.glob(os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', f'targetphot-potential-*-{specprod}.fits'))
    if redshift_type == 'base' or redshift_type == 'patch':
        redshift_dir = os.path.join(options.datapath, 'spectro', 'redux', specprod, 'zcatalog')
        if redshift_type == 'base':
            zpix_file = os.path.join(redshift_dir, f'zall-pix-{specprod}.fits')
            ztile_file = os.path.join(redshift_dir, f'zall-tilecumulative-{specprod}.fits')
        else:
            zpix_file = os.path.join(redshift_dir, redshift_version, f'zall-pix-{specprod}.fits')
            ztile_file = os.path.join(redshift_dir, redshift_version, f'zall-tilecumulative-{specprod}.fits')
    elif redshift_type == 'zcat':
        redshift_dir = os.path.join(options.datapath, 'vac', release, 'zcat', specprod)
        zpix_file = os.path.join(redshift_dir, redshift_version, f'zall-pix-{release}-vac.fits')
        ztile_file = os.path.join(redshift_dir, redshift_version, f'zall-tilecumulative-{release}-vac.fits')
    elif redshift_type == 'daily':
        redshift_dir = os.path.join(options.datapath, 'spectro', 'redux', specprod)
        zpix_file = 'Daily loads are not meant to include Healpix redshifts.'
        ztile_file = 'Only cumulative redshifts are needed for daily loads.'
    else:
        log.critical("Unsupported redshift catalog type: '%s'!", redshift_type)
        return 1
    if specprod == 'daily':
        tiles_type = 'csv'
    else:
        tiles_type = 'fits'
    tiles_version = config[specprod]['tiles']
    chunksize = config[specprod].getint('chunksize')
    loaders = {'exposures': [{'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'tiles-{specprod}.{tiles_type}'),
                              'tcls': Tile,
                              'hdu': 'TILE_COMPLETENESS',  # Ignored for CSV files.
                              'q3c': 'tilera',
                              'chunksize': chunksize
                              },
                             {'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'exposures-{specprod}.fits'),
                              'tcls': Exposure,
                              'hdu': 'EXPOSURES',
                              'q3c': 'tilera',
                              'chunksize': chunksize
                              },
                             {'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'exposures-{specprod}.fits'),
                              'tcls': Frame,
                              'hdu': 'FRAMES',
                              'chunksize': chunksize
                              }],
               #
               # The potential targets are supposed to include data for all targets.
               # In other words, every actual target is also a potential target.
               #
               'photometry': [{'filepaths': glob.glob(os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', 'tractorphot', 'tractorphot*.fits')),
                               'tcls': Photometry,
                               'hdu': 'TRACTORPHOT',
                               'chunksize': chunksize
                               }],
               #
               # This stage loads targets, and such photometry as they have, that did not
               # successfully match to a known LS DR9 object.
               #
               'targetphot': [{'filepaths': target_files,
                               'tcls': Photometry,
                               'hdu': 'TARGETPHOT',
                               'row_filter': deduplicate_targetid,
                               'q3c': 'ra',
                               'chunksize': chunksize
                               }],
               'target': [{'filepaths': target_files,
                           'tcls': Target,
                           'hdu': 'TARGETPHOT',
                           'chunksize': chunksize
                           }],
               'redshift': [{'filepaths': ztile_file,
                             'tcls': Ztile,
                             'hdu': 'ZCATALOG',
                             'row_filter': no_sky,
                             'chunksize': chunksize
                             }],
               'fiberassign': [{'filepaths': None,
                                'tcls': Fiberassign,
                                'hdu': 'FIBERASSIGN',
                                'row_filter': no_sky,
                                'q3c': 'target_ra',
                                'chunksize': chunksize
                                },
                               {'filepaths': None,
                                'tcls': Potential,
                                'hdu': 'POTENTIAL_ASSIGNMENTS',
                                'row_filter': no_sky,
                                'chunksize': chunksize
                                }]}
    if specprod != 'daily':
        loaders['redshift'].append({'filepaths': zpix_file,
                                    'tcls': Zpix,
                                    'hdu': 'ZCATALOG',
                                    'row_filter': no_sky,
                                    'chunksize': chunksize
                                    })
    try:
        loader = loaders[options.load]
    except KeyError:
        log.critical("Unknown loading stage '%s'!", options.load)
        return 1
    #
    # Find the tiles that need to be loaded. Not all fiberassign files are compressed!
    #
    if options.load == 'fiberassign':
        fiberassign_search_dirs = [os.path.join(options.datapath, 'target', 'fiberassign', 'tiles', 'tags', tiles_version),
                                   os.path.join(options.datapath, 'target', 'fiberassign', 'tiles', tiles_version),
                                   os.path.join('/global/cfs/cdirs/desi', 'target', 'fiberassign', 'tiles', 'tags', tiles_version),
                                   os.path.join('/global/cfs/cdirs/desi', 'target', 'fiberassign', 'tiles', tiles_version),
                                   os.path.join('/global/cfs/cdirs/desi', 'target', 'fiberassign', 'tiles', 'branches', tiles_version)]
        for d in fiberassign_search_dirs:
            if os.path.isdir(d):
                fiberassign_dir = d
                log.info('Found fiberassign directory: %s.', fiberassign_dir)
                break
        try:
            fiberassign_files = [checkgzip(os.path.join(fiberassign_dir, (f"{tileid[0]:06d}")[0:3], f"fiberassign-{tileid[0]:06d}.fits"))
                                 for tileid in dbSession.query(Tile.tileid).order_by(Tile.tileid)]
        except FileNotFoundError:
            log.error("Some fiberassign files were not found!")
            return 1
        log.debug(fiberassign_files)
        for k in range(len(loader)):
            loader[k]['filepaths'] = fiberassign_files
    #
    # Load the tables that correspond to a set of files.
    #
    if options.load == 'exposures' and options.overwrite:
        load_versions(photometry_version, f"{redshift_type}/{redshift_version}",
                      release, specprod, tiles_version)
    for l in loader:
        tn = l['tcls'].__tablename__
        loaded = dbSession.query(l['tcls']).count()
        #
        # The targetphot stage adds to the existing photometry table.
        #
        if loaded > 0 and options.load != 'targetphot':
            log.info("Loading appears to be complete on %s.", tn)
        else:
            log.info("Loading %s from %s.", tn, str(l['filepaths']))
            load_file(**l)
            log.info("Finished loading %s.", tn)
    if options.load == 'fiberassign':
        log.info("Consider running VACUUM FULL VERBOSE ANALYZE at this point.")
    #
    # Clean up.
    #
    dbSession.close()
    engine.dispose()
    return 0
