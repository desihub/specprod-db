# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.load
===============

Code for loading a spectroscopic production database. This includes both
targeting and redshift data.

Notes
-----

- Plan for how to support fuji+guadalupe combined analysis.  May need to look
  into cross-schema views, or daughter tables that inherit from both schemas.
- Plan for loading daily reductions, in addition to static reductions.
- Anticipate loading afterburners and VACs into the database.
- Load redshifts from all redrock files in ``tiles/cumulative``, rather than
  from the ``ztile-*-cumulative.fits`` summary file.
"""
import os
# import re
import glob
import itertools
# import sys
from configparser import SafeConfigParser

from pkg_resources import resource_filename

import numpy as np
from astropy import __version__ as astropy_version
# from astropy.io import fits
from astropy.table import Table, MaskedColumn, join
from astropy.time import Time
from pytz import utc

from sqlalchemy import __version__ as sqlalchemy_version
from sqlalchemy import (create_engine, event, ForeignKey, Column, DDL,
                        BigInteger, Boolean, Integer, String, Float, DateTime,
                        SmallInteger, bindparam, Numeric, and_)
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import (declarative_base, declarative_mixin, declared_attr,
                            scoped_session, sessionmaker, relationship)
from sqlalchemy.schema import CreateSchema, Index
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, REAL

from desiutil import __version__ as desiutil_version
from desiutil.iers import freeze_iers
from desiutil.log import get_logger, DEBUG, INFO

from . import __version__ as specprodDB_version
from .util import (convert_dateobs, parse_pgpass, cameraid, surveyid, programid,
                   spgrpid, checkgzip)


Base = declarative_base()
engine = None
dbSession = scoped_session(sessionmaker())
schemaname = None
log = None


@declarative_mixin
class SchemaMixin(object):
    """Mixin class to allow schema name to be changed at runtime. Also
    automatically sets the table name.
    """

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr
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


class Photometry(SchemaMixin, Base):
    """Contains *only* photometric quantities associated with a ``TARGETID``.

    This table is deliberately designed so that ``TARGETID`` can serve as a
    primary key. Any quantities created or modified by desitarget are
    defined in the :class:`~specprodTarget` class.

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


class Target(SchemaMixin, Base):
    """Representation of the pure-desitarget quantities in the
    ``TARGETPHOT`` table in the targetphot files.
    """
    @declared_attr
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
    cmx_target = Column(BigInteger, nullable=False)
    desi_target = Column(BigInteger, nullable=False)
    bgs_target = Column(BigInteger, nullable=False)
    mws_target = Column(BigInteger, nullable=False)
    sv1_desi_target = Column(BigInteger, nullable=False)
    sv1_bgs_target = Column(BigInteger, nullable=False)
    sv1_mws_target = Column(BigInteger, nullable=False)
    sv2_desi_target = Column(BigInteger, nullable=False)
    sv2_bgs_target = Column(BigInteger, nullable=False)
    sv2_mws_target = Column(BigInteger, nullable=False)
    sv3_desi_target = Column(BigInteger, nullable=False)
    sv3_bgs_target = Column(BigInteger, nullable=False)
    sv3_mws_target = Column(BigInteger, nullable=False)
    scnd_target = Column(BigInteger, nullable=False)
    sv1_scnd_target = Column(BigInteger, nullable=False)
    sv2_scnd_target = Column(BigInteger, nullable=False)
    sv3_scnd_target = Column(BigInteger, nullable=False)
    survey = Column(String(7), nullable=False, index=True)
    program = Column(String(6), nullable=False, index=True)
    tileid = Column(Integer, ForeignKey('tile.tileid'), nullable=False, index=True)  # fiberassign

    photometry = relationship("Photometry", back_populates="targets")
    tile = relationship("Tile", back_populates="targets")

    def __repr__(self):
        return "Target(targetid={0.targetid:d}, tileid={0.tileid:d}, survey='{0.survey}')".format(self)


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
    goaltype = Column(String(20), nullable=False)
    mintfrac = Column(DOUBLE_PRECISION, nullable=False)
    lastnight = Column(Integer, nullable=False)  # In principle this could be replaced by MAX(night) grouped by exposures.

    exposures = relationship("Exposure", back_populates="tile")
    fiberassign = relationship("Fiberassign", back_populates="tile")
    potential = relationship("Potential", back_populates="tile")
    targets = relationship("Target", back_populates="tile")
    ztile_redshifts = relationship("Ztile", back_populates="tile")

    def __repr__(self):
        return "Tile(tileid={0.tileid:d})".format(self)


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
    goaltype = Column(String(6), nullable=False)
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
    @declared_attr
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


class Potential(SchemaMixin, Base):
    """Representation of the POTENTIAL_ASSIGNMENTS table in a fiberassign file.
    """
    @declared_attr
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


class Zpix(SchemaMixin, Base):
    """Representation of the ``ZCATALOG`` table in zpix files.
    """
    @declared_attr
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "survey", "program", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
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
    cmx_target = Column(BigInteger, nullable=False)
    desi_target = Column(BigInteger, nullable=False)
    bgs_target = Column(BigInteger, nullable=False)
    mws_target = Column(BigInteger, nullable=False)
    scnd_target = Column(BigInteger, nullable=False)
    sv1_desi_target = Column(BigInteger, nullable=False)
    sv1_bgs_target = Column(BigInteger, nullable=False)
    sv1_mws_target = Column(BigInteger, nullable=False)
    sv1_scnd_target = Column(BigInteger, nullable=False)
    sv2_desi_target = Column(BigInteger, nullable=False)
    sv2_bgs_target = Column(BigInteger, nullable=False)
    sv2_mws_target = Column(BigInteger, nullable=False)
    sv2_scnd_target = Column(BigInteger, nullable=False)
    sv3_desi_target = Column(BigInteger, nullable=False)
    sv3_bgs_target = Column(BigInteger, nullable=False)
    sv3_mws_target = Column(BigInteger, nullable=False)
    sv3_scnd_target = Column(BigInteger, nullable=False)
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


class Ztile(SchemaMixin, Base):
    """Representation of the ``ZCATALOG`` table in ztile files.
    """
    @declared_attr
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "spgrp", "spgrpval", "tileid", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetphotid = Column(Numeric(39), ForeignKey("target.id"), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
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


def _frameid(data):
    """Update the ``frameid`` column.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`astropy.table.Table`
        Updated data table.
    """
    frameid = 100*data['EXPID'] + np.array([cameraid(c) for c in data['CAMERA']], dtype=data['EXPID'].dtype)
    data.add_column(frameid, name='FRAMEID', index=0)
    return data


def _tileid(data):
    """Update the ``tileid`` column.  Also check for the presence of ``PLATE_RA``, ``PLATE_DEC``.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`astropy.table.Table`
        Updated data table.
    """
    try:
        tileid = data.meta['TILEID']*np.ones(len(data), dtype=np.int32)
    except KeyError:
        log.error("Could not find TILEID in metadata!")
        raise
    data.add_column(tileid, name='TILEID', index=0)
    if 'TARGET_RA' in data.colnames and 'PLATE_RA' not in data.colnames:
        log.debug("Adding PLATE_RA, PLATE_DEC.")
        data['PLATE_RA'] = data['TARGET_RA']
        data['PLATE_DEC'] = data['TARGET_DEC']
    id0 = data['LOCATION'].base.astype(np.int64) << 32 | data['TILEID'].base.astype(np.int64)
    composite_id = np.array([id0, data['TARGETID'].base]).T
    data.add_column(composite_id, name='ID', index=0)
    return data


def _survey_program(data):
    """Add ``SURVEY``, ``PROGRAM``, ``SPGRP`` columns to zpix and ztile tables.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`astropy.table.Table`
        Updated data table.

    Raises
    ------
    KeyError
        If a necessary header could not be found.
    """
    # for i, key in enumerate(('SURVEY', 'PROGRAM', 'SPGRP')):
    for i, key in enumerate(('SURVEY', 'PROGRAM')):
        if key in data.colnames:
            log.info("Column %s is already in the table.", key)
        else:
            try:
                val = data.meta[key]
            except KeyError:
                log.error("Could not find %s in metadata!", key)
                raise
            log.debug("Adding %s column.", key)
            data.add_column(np.array([val]*len(data)), name=key, index=i+1)
    # objid, brickid, release, mock, sky, gaiadr = decode_targetid(data['TARGETID'])
    # data.add_column(sky, name='SKY', index=0)
    if 'FIRSTNIGHT' not in data.colnames:
        log.info("Adding FIRSTNIGHT column")
        data.add_column(np.array([0]*len(data), dtype=np.int32), name='FIRSTNIGHT', index=data.colnames.index('PROGRAM')+1)
    if 'LASTNIGHT' not in data.colnames:
        log.info("Adding LASTNIGHT column")
        data.add_column(np.array([0]*len(data), dtype=np.int32), name='LASTNIGHT', index=data.colnames.index('PROGRAM')+2)
    if 'MAIN_NSPEC' not in data.colnames:
        data.add_column(np.array([0]*len(data), dtype=np.int16), name='MAIN_NSPEC', index=data.colnames.index('SV_PRIMARY')+1)
        data.add_column(np.array([False]*len(data), dtype=np.int16), name='MAIN_PRIMARY', index=data.colnames.index('MAIN_NSPEC')+1)
    if 'SV_NSPEC' not in data.colnames:
        data.add_column(np.array([0]*len(data), dtype=np.int16), name='SV_NSPEC', index=data.colnames.index('TSNR2_LRG')+1)
        data.add_column(np.array([False]*len(data), dtype=np.int16), name='SV_PRIMARY', index=data.colnames.index('SV_NSPEC')+1)
    #
    # Reductions like guadalupe may not have the full set of target bitmasks
    #
    surveys = ('', 'sv1', 'sv2', 'sv3')
    programs = ('desi', 'bgs', 'mws', 'scnd')
    masks = ['cmx_target'] + [('_'.join(p) if p[0] else p[1]) + '_target'
                              for p in itertools.product(surveys, programs)]
    mask_index = data.colnames.index('NUMOBS_INIT') + 1
    for mask in masks:
        if mask.upper() not in data.colnames:
            log.info("Adding %s at index %d.", mask.upper(), mask_index)
            data.add_column(np.array([0]*len(data), dtype=np.int64), name=mask.upper(), index=mask_index)
        mask_index += 1
    if 'TILEID' in data.colnames:
        data.add_column(np.array(['cumulative']*len(data)), name='SPGRP', index=data.colnames.index('PROGRAM')+1)
        data = _target_unique_id(data)
        data.rename_column('ID', 'TARGETPHOTID')
        s = np.array([spgrpid(s) for s in data['SPGRP']], dtype=np.int64)
        id0 = (s << 27 | data['SPGRPVAL'].base.astype(np.int64)) << 32 | data['TILEID'].base.astype(np.int64)
    else:
        data.add_column(np.array(['healpix']*len(data)), name='SPGRP', index=data.colnames.index('PROGRAM')+1)
        s = np.array([surveyid(s) for s in data['SURVEY']], dtype=np.int64)
        p = np.array([programid(s) for s in data['PROGRAM']], dtype=np.int64)
        id0 = p << 32 | s
    composite_id = np.array([id0, data['TARGETID'].base]).T
    data.add_column(composite_id, name='ID', index=0)
    return data


def _target_unique_id(data):
    """Add composite ``ID`` column for later conversion.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`astropy.table.Table`
        Updated data table.
    """
    s = np.array([surveyid(s) for s in data['SURVEY']], dtype=np.int64)
    id0 = s << 32 | data['TILEID'].base.astype(np.int64)
    composite_id = np.array([id0, data['TARGETID'].base]).T
    data.add_column(composite_id, name='ID', index=0)
    return data


def _add_ls_id(data):
    """Add LS_ID to targetphot data.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`astropy.table.Table`
        Updated data table.
    """
    ls_id = ((data['RELEASE'].data.astype(np.int64) << 40) |
             (data['BRICKID'].data.astype(np.int64) << 16) |
             data['BRICK_OBJID'].data.astype(np.int64))
    data.add_column(ls_id, name='LS_ID', index=0)
    return data


def _deduplicate_targetid(data):
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


def _remove_loaded_targetid(data):
    """Remove rows with TARGETID already loaded into the database.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`numpy.ndarray`
        An array of rows that are safe to load.
    """
    targetid = data['TARGETID'].data
    good_rows = np.ones((len(targetid),), dtype=bool)
    q = dbSession.query(Photometry.targetid).filter(Photometry.targetid.in_(targetid.tolist())).all()
    for row in q:
        good_rows[targetid == row[0]] = False
    return good_rows


def _remove_loaded_unique_id(data):
    """Remove rows with UNIQUE ID already loaded into the database.

    Parameters
    ----------
    data : :class:`astropy.table.Table`
        The initial data read from the file.

    Returns
    -------
    :class:`numpy.ndarray`
        An array of rows that are safe to load.
    """
    rows = dbSession.query(Target.id).all()
    loaded_id = [r[0] for r in rows]
    data_id = [(int(data['ID'][k][0]) << 64) | int(data['ID'][k][1])
               for k in range(len(data))]
    id_index = dict(zip(data_id, range(len(data))))
    good_rows = np.ones((len(data),), dtype=bool)
    for i in loaded_id:
        good_rows[id_index[i]] = False
    return good_rows


def load_file(filepaths, tcls, hdu=1, preload=None, expand=None, insert=None, convert=None,
              index=None, rowfilter=None, q3c=None,
              chunksize=50000, maxrows=0):
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
    preload : callable, optional
        A function that takes a :class:`~astropy.table.Table` as an argument.
        Use this for more complicated manipulation of the data before loading,
        for example a function that depends on multiple columns. The return
        value should be the updated Table.
    expand : :class:`dict`, optional
        If set, map FITS column names to one or more alternative column names.
    insert : :class:`dict`, optional
        If set, insert one or more columns, before an existing column. The
        existing column will be copied into the new column(s).
    convert : :class:`dict`, optional
        If set, convert the data for a named (database) column using the
        supplied function.
    index : :class:`str`, optional
        If set, add a column that just counts the number of rows.
    rowfilter : callable, optional
        If set, apply this filter to the rows to be loaded.  The function
        should return :class:`bool`, with ``True`` meaning a good row.
    q3c : :class:`str`, optional
        If set, create q3c index on the table, using the RA column
        named `q3c`.
    chunksize : :class:`int`, optional
        If set, load database `chunksize` rows at a time (default 50000).
    maxrows : :class:`int`, optional
        If set, stop loading after `maxrows` are loaded.  Alteratively,
        set `maxrows` to zero (0) to load all rows.

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
        if maxrows == 0 or len(data) < maxrows:
            mr = len(data)
        else:
            mr = maxrows
        if preload is not None:
            data = preload(data)
            log.info("Preload function complete on %s.", tn)
        try:
            colnames = data.names
        except AttributeError:
            colnames = data.colnames
        masked = dict()
        for col in colnames:
            if data[col].dtype.kind == 'f':
                if isinstance(data[col], MaskedColumn):
                    bad = np.isnan(data[col].data.data[0:mr])
                    masked[col] = True
                else:
                    bad = np.isnan(data[col][0:mr])
                if np.any(bad):
                    if bad.ndim == 1:
                        log.warning("%d rows of bad data detected in column " +
                                    "%s of %s.", bad.sum(), col, filepath)
                    elif bad.ndim == 2:
                        nbadrows = len(bad.sum(1).nonzero()[0])
                        nbaditems = bad.sum(1).sum()
                        log.warning("%d rows (%d items) of bad data detected in column " +
                                    "%s of %s.", nbadrows, nbaditems, col, filepath)
                    else:
                        log.warning("Bad data detected in high-dimensional column %s of %s.", col, filepath)
                    #
                    # TODO: is this replacement appropriate for all columns?
                    #
                    if col in masked:
                        data[col].data.data[0:mr][bad] = -9999.0
                    else:
                        data[col][0:mr][bad] = -9999.0
        log.info("Integrity check complete on %s.", tn)
        if rowfilter is None:
            good_rows = np.ones((mr,), dtype=bool)
        else:
            good_rows = rowfilter(data[0:mr])
        if good_rows.sum() == 0:
            log.info("Row filter removed all data rows, skipping %s.", filepath)
            continue
        log.info("Row filter applied on %s; %d rows remain.", tn, good_rows.sum())
        data_list = list()
        for col in colnames:
            if col in masked:
                data_list.append(data[col].data.data[0:mr][good_rows].tolist())
            else:
                data_list.append(data[col][0:mr][good_rows].tolist())
        data_names = [col.lower() for col in colnames]
        finalrows = len(data_list[0])
        log.info("Initial column conversion complete on %s.", tn)
        if expand is not None:
            for col in expand:
                i = data_names.index(col.lower())
                if isinstance(expand[col], str):
                    #
                    # Just rename a column.
                    #
                    log.debug("Renaming column %s (at index %d) to %s.", data_names[i], i, expand[col])
                    data_names[i] = expand[col]
                else:
                    #
                    # Assume this is an expansion of an array-valued column
                    # into individual columns.
                    #
                    del data_names[i]
                    del data_list[i]
                    for j, n in enumerate(expand[col]):
                        log.debug("Expanding column %d of %s (at index %d) to %s.", j, col, i, n)
                        data_names.insert(i + j, n)
                        data_list.insert(i + j, data[col][:, j].tolist())
                    log.debug(data_names)
            log.info("Column expansion complete on %s.", tn)
        del data
        if insert is not None:
            for col in insert:
                i = data_names.index(col)
                for item in insert[col]:
                    data_names.insert(i, item)
                    data_list.insert(i, data_list[i].copy())  # Dummy values
            log.info("Column insertion complete on %s.", tn)
        if convert is not None:
            for col in convert:
                i = data_names.index(col)
                data_list[i] = [convert[col](x) for x in data_list[i]]
            log.info("Column conversion complete on %s.", tn)
        if index is not None:
            data_list.insert(0, list(range(1, finalrows+1)))
            data_names.insert(0, index)
            log.info("Added index column '%s'.", index)
        data_rows = list(zip(*data_list))
        del data_list
        log.info("Converted columns into rows on %s.", tn)
        n_chunks = finalrows//chunksize
        if finalrows % chunksize:
            n_chunks += 1
        for k in range(n_chunks):
            data_chunk = [dict(zip(data_names, row))
                          for row in data_rows[k*chunksize:(k+1)*chunksize]]
            if len(data_chunk) > 0:
                loaded_rows += len(data_chunk)
                engine.execute(tcls.__table__.insert(), data_chunk)
                log.info("Inserted %d rows in %s.",
                         min((k+1)*chunksize, finalrows), tn)
            else:
                log.error("Detected empty data chunk in %s!", tn)
        # for k in range(finalrows//chunksize + 1):
        #     data_insert = [dict([(col, data_list[i].pop(0))
        #                          for i, col in enumerate(data_names)])
        #                    for j in range(chunksize)]
        #     session.bulk_insert_mappings(tcls, data_insert)
        #     log.info("Inserted %d rows in %s..",
        #              min((k+1)*chunksize, finalrows), tn)
        dbSession.commit()
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
    dbSession.execute(q3c_sql)
    log.info("Finished q3c index on %s.%s.", schemaname, table)
    dbSession.commit()
    return


def zpix_target(specprod):
    """Replace targeting bitmasks in the redshift tables for `specprod`.

    Parameters
    ----------
    specprod : :class:`str`
        The spectroscopic production, normally the value of :envvar:`SPECPROD`.
    """
    specprod_survey_program = {'fuji': {'cmx': ('other', ),
                                        'special': ('dark', ),
                                        'sv1': ('backup', 'bright', 'dark', 'other'),
                                        'sv2': ('backup', 'bright', 'dark'),
                                        'sv3': ('backup', 'bright', 'dark')},
                               'guadalupe': {'special': ('bright', 'dark'),
                                             'main': ('bright', 'dark')},
                               'iron': {'cmx': ('other', ),
                                        'main': ('backup', 'bright', 'dark'),
                                        'special': ('backup', 'bright', 'dark', 'other'),
                                        'sv1': ('backup', 'bright', 'dark', 'other'),
                                        'sv2': ('backup', 'bright', 'dark'),
                                        'sv3': ('backup', 'bright', 'dark')}}
    target_bits = {'cmx': {'cmx': Target.cmx_target},
                   'sv1': {'desi': Target.sv1_desi_target, 'bgs': Target.sv1_bgs_target, 'mws': Target.sv1_mws_target},
                   'sv2': {'desi': Target.sv2_desi_target, 'bgs': Target.sv2_bgs_target, 'mws': Target.sv2_mws_target},
                   'sv3': {'desi': Target.sv3_desi_target, 'bgs': Target.sv3_bgs_target, 'mws': Target.sv3_mws_target},
                   'main': {'desi': Target.desi_target, 'bgs': Target.bgs_target, 'mws': Target.mws_target},
                   'special': {'desi': Target.desi_target, 'bgs': Target.bgs_target, 'mws': Target.mws_target}}
    #
    # Find targetid assigned to multiple tiles.
    #
    assigned_multiple_tiles = dict()
    for survey in specprod_survey_program[specprod]:
        assigned_multiple_tiles[survey] = dict()
        for program in specprod_survey_program[specprod][survey]:
            assigned_multiple_tiles[survey][program] = dbSession.query(Target.targetid).join(Fiberassign,
                                                                                             and_(Target.targetid == Fiberassign.targetid,
                                                                                                  Target.tileid == Fiberassign.tileid)).filter(Target.survey == survey).filter(Target.program == program).group_by(Target.targetid).having(func.count(Target.tileid) > 1)
    #
    # From that set, find cases targetid and a targeting bit are distinct pairs.
    #
    distinct_target = dict()
    for survey in assigned_multiple_tiles:
        distinct_target[survey] = dict()
        for program in assigned_multiple_tiles[survey]:
            distinct_target[survey][program] = dict()
            for bits in target_bits[survey]:
                distinct_target[survey][program][bits] = dbSession.query(Target.targetid, target_bits[survey][bits]).filter(Target.targetid.in_(assigned_multiple_tiles[survey][program])).filter(Target.survey == survey).filter(Target.program == program).distinct().subquery()
    #
    # Obtain the list of targetids where a targeting bit appears more than once with different values.
    #
    multiple_target = dict()
    for survey in distinct_target:
        multiple_target[survey] = dict()
        for program in distinct_target[survey]:
            multiple_target[survey][program] = dict()
            for bits in distinct_target[survey][program]:
                if survey.startswith('sv'):
                    column = getattr(distinct_target[survey][program][bits].c, f"{survey}_{bits}_target")
                elif survey == 'cmx':
                    column = distinct_target[survey][program][bits].c.cmx_target
                else:
                    column = getattr(distinct_target[survey][program][bits].c, f"{bits}_target")
                multiple_target[survey][program][bits] = [row[0] for row in dbSession.query(distinct_target[survey][program][bits].c.targetid).group_by(distinct_target[survey][program][bits].c.targetid).having(func.count(column) > 1).all()]
    #
    # Consolidate the list of targetids.
    #
    targetids_to_fix = dict()
    for survey in multiple_target:
        for program in multiple_target[survey]:
            for bits in multiple_target[survey][program]:
                if multiple_target[survey][program][bits]:
                    if survey not in targetids_to_fix:
                        targetids_to_fix[survey] = dict()
                    if program in targetids_to_fix[survey]:
                        log.debug("targetids_to_fix['%s']['%s'] += multiple_target['%s']['%s']['%s']",
                                  survey, program, survey, program, bits)
                        targetids_to_fix[survey][program] += multiple_target[survey][program][bits]
                    else:
                        log.debug("targetids_to_fix['%s']['%s'] = multiple_target['%s']['%s']['%s']",
                                  survey, program, survey, program, bits)
                        targetids_to_fix[survey][program] = multiple_target[survey][program][bits]
    #
    # ToO observations that had targeting bits zeroed out.
    #
    if specprod == 'fuji':
        #
        # Maybe this problem only affects fuji, but need to confirm that.
        #
        zero_ToO = dict()
        for survey in specprod_survey_program[specprod]:
            zero_ToO[survey] = dict()
            for program in specprod_survey_program[specprod][survey]:
                zero_ToO[survey][program] = [row[0] for row in dbSession.query(Zpix.targetid).filter((Zpix.targetid.op('&')((2**16 - 1) << 42)).op('>>')(42) == 9999).filter(Zpix.survey == survey).filter(Zpix.program == program).all()]
        for survey in zero_ToO:
            for program in zero_ToO[survey]:
                if zero_ToO[survey][program]:
                    if survey not in targetids_to_fix:
                        targetids_to_fix[survey] = dict()
                    if program in targetids_to_fix[survey]:
                        log.debug("targetids_to_fix['%s']['%s'] += zero_ToO['%s']['%s']",
                                  survey, program, survey, program)
                        targetids_to_fix[survey][program] += zero_ToO[survey][program]
                    else:
                        log.debug("targetids_to_fix['%s']['%s'] = zero_ToO['%s']['%s']",
                                  survey, program, survey, program)
                        targetids_to_fix[survey][program] = zero_ToO[survey][program]
    #
    # Generate the query to obtain the bitwise-or of each targeting bit.
    #
    # table = 'zpix'
    surveys = ('', 'sv1', 'sv2', 'sv3')
    programs = ('desi', 'bgs', 'mws', 'scnd')
    masks = ['cmx_target'] + [('_'.join(p) if p[0] else p[1]) + '_target'
                              for p in itertools.product(surveys, programs)]
    bit_or_query = dict()
    for survey in targetids_to_fix:
        bit_or_query[survey] = dict()
        for program in targetids_to_fix[survey]:
            log.debug("SELECT t.targetid, " +
                      ', '.join([f"BIT_OR(t.{m}) AS {m}" for m in masks]) +
                      f" FROM {specprod}.target AS t WHERE t.targetid IN ({', '.join(map(str, set(targetids_to_fix[survey][program])))}) AND t.survey = '{survey}' AND t.program = '{program}' GROUP BY t.targetid;")
            bq = ("dbSession.query(Target.targetid, " +
                  ', '.join([f"func.bit_or(Target.{m}).label('{m}')" for m in masks]) +
                  f").filter(Target.targetid.in_([{', '.join(map(str, set(targetids_to_fix[survey][program])))}])).filter(Target.survey == '{survey}').filter(Target.program == '{program}').group_by(Target.targetid)")
            log.debug(bq)
            bit_or_query[survey][program] = eval(bq)
    #
    # Apply the updates
    #
    # update_string = '{' + ', '.join([f"Zpix.{m}: {{0.{m}:d}}" for m in masks]) + '}'
    for survey in bit_or_query:
        for program in bit_or_query[survey]:
            for row in bit_or_query[survey][program].all():
                zpix_match = dbSession.query(Zpix).filter(Zpix.targetid == row.targetid).filter(Zpix.survey == survey).filter(Zpix.program == program).one()
                for m in masks:
                    log.info("%s.%s = %s", zpix_match, m, str(getattr(row, m)))
                zpix_match.cmx_target = row.cmx_target
                zpix_match.desi_target = row.desi_target
                zpix_match.bgs_target = row.bgs_target
                zpix_match.mws_target = row.mws_target
                zpix_match.scnd_target = row.scnd_target
                zpix_match.sv1_desi_target = row.sv1_desi_target
                zpix_match.sv1_bgs_target = row.sv1_bgs_target
                zpix_match.sv1_mws_target = row.sv1_mws_target
                zpix_match.sv1_scnd_target = row.sv1_scnd_target
                zpix_match.sv2_desi_target = row.sv2_desi_target
                zpix_match.sv2_bgs_target = row.sv2_bgs_target
                zpix_match.sv2_mws_target = row.sv2_mws_target
                zpix_match.sv2_scnd_target = row.sv2_scnd_target
                zpix_match.sv3_desi_target = row.sv3_desi_target
                zpix_match.sv3_bgs_target = row.sv3_bgs_target
                zpix_match.sv3_mws_target = row.sv3_mws_target
                zpix_match.sv3_scnd_target = row.sv3_scnd_target
                dbSession.commit()
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


def get_options():
    """Parse command-line options.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed options.
    """
    from sys import argv
    from argparse import ArgumentParser
    prsr = ArgumentParser(description=("Load redshift data into a database."),
                          prog=os.path.basename(argv[0]))
    prsr.add_argument('-c', '--config', action='store', dest='config', metavar='FILE',
                      default=resource_filename('specprodDB', 'data/load_specprod_db.ini'),
                      help="Override the default configuration file.")
    # prsr.add_argument('-d', '--data-release', action='store', dest='release',
    #                   default='edr', metavar='RELEASE',
    #                   help='Use data release RELEASE (default "%(default)s").')
    prsr.add_argument('-f', '--filename', action='store', dest='dbfile',
                      default='specprod.db', metavar='FILE',
                      help='Store data in FILE (default "%(default)s").')
    # prsr.add_argument('-H', '--hostname', action='store', dest='hostname',
    #                   metavar='HOSTNAME', default='specprod-db.desi.lbl.gov',
    #                   help='If specified, connect to a PostgreSQL database on HOSTNAME (default "%(default)s").')
    prsr.add_argument('-l', '--load', action='store', dest='load',
                      default='exposures', metavar='STAGE',
                      help='Load the set of files associated with STAGE (default "%(default)s").')
    # prsr.add_argument('-m', '--max-rows', action='store', dest='maxrows',
    #                   type=int, default=0, metavar='M',
    #                   help="Load up to M rows in the tables (default is all rows).")
    prsr.add_argument('-o', '--overwrite', action='store_true', dest='overwrite',
                      help='Delete any existing files or tables before loading.')
    prsr.add_argument('-P', '--public', action='store_true', dest='public',
                      help='GRANT access to the schema to the public database user.')
    # prsr.add_argument('-p', '--photometry-version', action='store', dest='photometry_version',
    #                   metavar='VERSION', default='v2.1',
    #                   help='Load target photometry data from VERSION (default "%(default)s").')
    # prsr.add_argument('-r', '--rows', action='store', dest='chunksize',
    #                   type=int, default=50000, metavar='N',
    #                   help="Load N rows at a time (default %(default)s).")
    prsr.add_argument('-s', '--schema', action='store', dest='schema',
                      metavar='SCHEMA',
                      help='Set the schema name in the PostgreSQL database.')
    # prsr.add_argument('-t', '--tiles-version', action='store', dest='tiles_version',
    #                   metavar='VERSION', default='0.5',
    #                   help='Load fiberassign data from VERSION (default "%(default)s").')
    # prsr.add_argument('-U', '--username', action='store', dest='username',
    #                   metavar='USERNAME', default='desi_admin',
    #                   help='If specified, connect to a PostgreSQL database with USERNAME (default "%(default)s").')
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Print extra information.')
    # prsr.add_argument('-z', '--redshift-version', action='store', dest='redshift_version',
    #                   metavar='VERSION',
    #                   help='Load redshift data from VAC VERSION')
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
    else:
        log.critical("Unsupported redshift catalog type: '%s'!", redshift_type)
        return 1
    tiles_version = config[specprod]['tiles']
    chunksize = config[specprod].getint('chunksize')
    maxrows = config[specprod].getint('maxrows')
    loaders = {'exposures': [{'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, 'tiles-{specprod}.fits'.format(specprod=specprod)),
                              'tcls': Tile,
                              'hdu': 'TILE_COMPLETENESS',
                              'q3c': 'tilera',
                              'chunksize': chunksize,
                              'maxrows': maxrows
                              },
                             {'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'exposures-{specprod}.fits'),
                              'tcls': Exposure,
                              'hdu': 'EXPOSURES',
                              'insert': {'mjd': ('date_obs',)},
                              'convert': {'date_obs': lambda x: Time(x, format='mjd').to_value('datetime').replace(tzinfo=utc)},
                              'q3c': 'tilera',
                              'chunksize': chunksize,
                              'maxrows': maxrows
                              },
                             {'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'exposures-{specprod}.fits'),
                              'tcls': Frame,
                              'hdu': 'FRAMES',
                              'preload': _frameid,
                              'chunksize': chunksize,
                              'maxrows': maxrows
                              }],
               #
               # The potential targets are supposed to include data for all targets.
               # In other words, every actual target is also a potential target.
               #
               'photometry': [{'filepaths': glob.glob(os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', 'tractorphot', 'tractorphot*.fits')),
                               'tcls': Photometry,
                               'hdu': 'TRACTORPHOT',
                               'expand': {'DCHISQ': ('dchisq_psf', 'dchisq_rex', 'dchisq_dev', 'dchisq_exp', 'dchisq_ser',),
                                          'OBJID': 'brick_objid',
                                          'TYPE': 'morphtype'},
                               # 'rowfilter': _remove_loaded_targetid,
                               'chunksize': chunksize,
                               'maxrows': maxrows
                               }],
               #
               # This stage loads targets, and such photometry as they have, that did not
               # successfully match to a known LS DR9 object.
               #
               'targetphot': [{'filepaths': target_files,
                               'tcls': Photometry,
                               'hdu': 'TARGETPHOT',
                               'preload': _add_ls_id,
                               'expand': {'DCHISQ': ('dchisq_psf', 'dchisq_rex', 'dchisq_dev', 'dchisq_exp', 'dchisq_ser',)},
                               'convert': {'gaia_astrometric_params_solved': lambda x: int(x)},
                               'rowfilter': _deduplicate_targetid,
                               'q3c': 'ra',
                               'chunksize': chunksize,
                               'maxrows': maxrows
                               }],
               'target': [{'filepaths': target_files,
                           'tcls': Target,
                           'hdu': 'TARGETPHOT',
                           'preload': _target_unique_id,
                           'convert': {'id': lambda x: x[0] << 64 | x[1]},
                           # 'rowfilter': _remove_loaded_unique_id,
                           'chunksize': chunksize,
                           'maxrows': maxrows
                           }],
               'redshift': [{'filepaths': zpix_file,
                             'tcls': Zpix,
                             'hdu': 'ZCATALOG',
                             'preload': _survey_program,
                             'expand': {'COEFF': ('coeff_0', 'coeff_1', 'coeff_2', 'coeff_3', 'coeff_4',
                                                  'coeff_5', 'coeff_6', 'coeff_7', 'coeff_8', 'coeff_9',)},
                             'convert': {'id': lambda x: x[0] << 64 | x[1]},
                             'rowfilter': lambda x: (x['TARGETID'] > 0) & ((x['TARGETID'] & 2**59) == 0),
                             'chunksize': chunksize,
                             'maxrows': maxrows
                             },
                            {'filepaths': ztile_file,
                             'tcls': Ztile,
                             'hdu': 'ZCATALOG',
                             'preload': _survey_program,
                             'expand': {'COEFF': ('coeff_0', 'coeff_1', 'coeff_2', 'coeff_3', 'coeff_4',
                                                  'coeff_5', 'coeff_6', 'coeff_7', 'coeff_8', 'coeff_9',)},
                             'convert': {'id': lambda x: x[0] << 64 | x[1],
                                         'targetphotid': lambda x: x[0] << 64 | x[1]},
                             'rowfilter': lambda x: (x['TARGETID'] > 0) & ((x['TARGETID'] & 2**59) == 0),
                             'chunksize': chunksize,
                             'maxrows': maxrows
                             }],
               'fiberassign': [{'filepaths': None,
                                'tcls': Fiberassign,
                                'hdu': 'FIBERASSIGN',
                                'preload': _tileid,
                                'convert': {'id': lambda x: x[0] << 64 | x[1]},
                                'rowfilter': lambda x: (x['TARGETID'] > 0) & ((x['TARGETID'] & 2**59) == 0),
                                'q3c': 'target_ra',
                                'chunksize': chunksize,
                                'maxrows': maxrows
                                },
                               {'filepaths': None,
                                'tcls': Potential,
                                'hdu': 'POTENTIAL_ASSIGNMENTS',
                                'preload': _tileid,
                                'convert': {'id': lambda x: x[0] << 64 | x[1]},
                                'rowfilter': lambda x: (x['TARGETID'] > 0) & ((x['TARGETID'] & 2**59) == 0),
                                'chunksize': chunksize,
                                'maxrows': maxrows
                                }]}
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
        log.info("Loading version metadata.")
        versions = [Version(package='specprod-db', version=specprodDB_version),
                    Version(package='lsdr9-photometry', version=photometry_version),
                    Version(package='redshift', version=rsv),
                    Version(package='tiles', version=tiles_version),
                    Version(package='specprod', version=specprod),
                    Version(package='numpy', version=np.__version__),
                    Version(package='astropy', version=astropy_version),
                    Version(package='sqlalchemy', version=sqlalchemy_version),
                    Version(package='desiutil', version=desiutil_version)]
        dbSession.add_all(versions)
        dbSession.commit()
        log.info("Completed loading version metadata.")
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
    if options.load == 'fiberassign' and redshift_type not in ('patch', 'zcat'):
        #
        # Fiberassign table has to be loaded for this step.
        # Eventually we want to eliminate this entirely.
        #
        log.info("Applying target bitmask corrections for %s to zpix table.",
                 specprod)
        try:
            zpix_target(specprod)
        except ProgrammingError:
            log.critical("Failed target bitmask corrections for %s!",
                         specprod)
            return 1
        log.info("Finished target bitmask corrections for %s zpix table.",
                 specprod)
    return 0
