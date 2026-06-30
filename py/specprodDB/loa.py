# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.loa
==============

ORM definition for :envvar:`SPECPROD` = ``loa``.
"""
import itertools

import numpy as np
from astropy.time import Time
from pytz import utc

from sqlalchemy import (ForeignKey, Column, BigInteger, Boolean, Integer,
                        String, DateTime, SmallInteger, Numeric)
from sqlalchemy.orm import declared_attr, relationship
from sqlalchemy.schema import Index
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, REAL

from desiutil.log import get_logger
from desiutil.names import radec_to_desiname

from .util import (cameraid, surveyid, programid, spgrpid, finitize)
from .base import Base, schema_mixin_factory


schemaname = 'loa'
SchemaMixin = schema_mixin_factory(schemaname)


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
    release = Column(SmallInteger, nullable=False)
    brickid = Column(Integer, nullable=False)
    brickname = Column(String(8), nullable=False)
    brick_objid = Column(Integer, nullable=False)
    morphtype = Column(String(4), nullable=False)
    ra = Column(DOUBLE_PRECISION, nullable=False)
    ra_ivar = Column(REAL, nullable=False)
    dec = Column(DOUBLE_PRECISION, nullable=False)
    dec_ivar = Column(REAL, nullable=False)
    dchisq_psf = Column(REAL, nullable=False)
    dchisq_rex = Column(REAL, nullable=False)
    dchisq_dev = Column(REAL, nullable=False)
    dchisq_exp = Column(REAL, nullable=False)
    dchisq_ser = Column(REAL, nullable=False)
    ebv = Column(REAL, nullable=False)
    flux_g = Column(REAL, nullable=False)
    flux_r = Column(REAL, nullable=False)
    flux_z = Column(REAL, nullable=False)
    flux_ivar_g = Column(REAL, nullable=False)
    flux_ivar_r = Column(REAL, nullable=False)
    flux_ivar_z = Column(REAL, nullable=False)
    mw_transmission_g = Column(REAL, nullable=False)
    mw_transmission_r = Column(REAL, nullable=False)
    mw_transmission_z = Column(REAL, nullable=False)
    fracflux_g = Column(REAL, nullable=False)
    fracflux_r = Column(REAL, nullable=False)
    fracflux_z = Column(REAL, nullable=False)
    fracmasked_g = Column(REAL, nullable=False)
    fracmasked_r = Column(REAL, nullable=False)
    fracmasked_z = Column(REAL, nullable=False)
    fracin_g = Column(REAL, nullable=False)
    fracin_r = Column(REAL, nullable=False)
    fracin_z = Column(REAL, nullable=False)
    nobs_g = Column(SmallInteger, nullable=False)
    nobs_r = Column(SmallInteger, nullable=False)
    nobs_z = Column(SmallInteger, nullable=False)
    psfdepth_g = Column(REAL, nullable=False)
    psfdepth_r = Column(REAL, nullable=False)
    psfdepth_z = Column(REAL, nullable=False)
    galdepth_g = Column(REAL, nullable=False)
    galdepth_r = Column(REAL, nullable=False)
    galdepth_z = Column(REAL, nullable=False)
    flux_w1 = Column(REAL, nullable=False)
    flux_w2 = Column(REAL, nullable=False)
    flux_w3 = Column(REAL, nullable=False)
    flux_w4 = Column(REAL, nullable=False)
    flux_ivar_w1 = Column(REAL, nullable=False)
    flux_ivar_w2 = Column(REAL, nullable=False)
    flux_ivar_w3 = Column(REAL, nullable=False)
    flux_ivar_w4 = Column(REAL, nullable=False)
    mw_transmission_w1 = Column(REAL, nullable=False)
    mw_transmission_w2 = Column(REAL, nullable=False)
    mw_transmission_w3 = Column(REAL, nullable=False)
    mw_transmission_w4 = Column(REAL, nullable=False)
    allmask_g = Column(SmallInteger, nullable=False)
    allmask_r = Column(SmallInteger, nullable=False)
    allmask_z = Column(SmallInteger, nullable=False)
    fiberflux_g = Column(REAL, nullable=False)
    fiberflux_r = Column(REAL, nullable=False)
    fiberflux_z = Column(REAL, nullable=False)
    fibertotflux_g = Column(REAL, nullable=False)
    fibertotflux_r = Column(REAL, nullable=False)
    fibertotflux_z = Column(REAL, nullable=False)
    ref_epoch = Column(REAL, nullable=False)
    wisemask_w1 = Column(SmallInteger, nullable=False)
    wisemask_w2 = Column(SmallInteger, nullable=False)
    maskbits = Column(SmallInteger, nullable=False)
    # LC_...
    shape_r = Column(REAL, nullable=False)
    shape_e1 = Column(REAL, nullable=False)
    shape_e2 = Column(REAL, nullable=False)
    shape_r_ivar = Column(REAL, nullable=False)
    shape_e1_ivar = Column(REAL, nullable=False)
    shape_e2_ivar = Column(REAL, nullable=False)
    sersic = Column(REAL, nullable=False)
    sersic_ivar = Column(REAL, nullable=False)
    ref_id = Column(BigInteger, nullable=False)
    ref_cat = Column(String(2), nullable=False)
    gaia_phot_g_mean_mag = Column(REAL, nullable=False)
    gaia_phot_g_mean_flux_over_error = Column(REAL, nullable=False)
    gaia_phot_bp_mean_mag = Column(REAL, nullable=False)
    gaia_phot_bp_mean_flux_over_error = Column(REAL, nullable=False)
    gaia_phot_rp_mean_mag = Column(REAL, nullable=False)
    gaia_phot_rp_mean_flux_over_error = Column(REAL, nullable=False)
    gaia_phot_bp_rp_excess_factor = Column(REAL, nullable=False)
    gaia_duplicated_source = Column(Boolean, nullable=False)
    gaia_astrometric_sigma5d_max = Column(REAL, nullable=False)
    gaia_astrometric_params_solved = Column(SmallInteger, nullable=False)
    parallax = Column(REAL, nullable=False)
    parallax_ivar = Column(REAL, nullable=False)
    pmra = Column(REAL, nullable=False)
    pmra_ivar = Column(REAL, nullable=False)
    pmdec = Column(REAL, nullable=False)
    pmdec_ivar = Column(REAL, nullable=False)
    targetid = Column(BigInteger, primary_key=True, autoincrement=False)

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
    targetid = Column(BigInteger, ForeignKey('photometry.targetid'), nullable=False, index=True)
    photsys = Column(String(1), nullable=False)
    subpriority = Column(DOUBLE_PRECISION, nullable=False)
    obsconditions = Column(BigInteger, nullable=False)
    priority_init = Column(BigInteger, nullable=False)
    numobs_init = Column(BigInteger, nullable=False)
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
    tileid = Column(Integer, ForeignKey('tile.tileid'), nullable=False, index=True)

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
        log = get_logger()
        if row_index is None:
            row_index = np.arange(len(data))
        if len(row_index) == 0:
            return []
        data = finitize(data)
        default_columns = dict()
        #
        # Surveys like main may not have the full set of target bitmasks
        #
        surveys = ('', 'sv1', 'sv2', 'sv3')
        programs = ('desi', 'bgs', 'mws', 'scnd')
        masks = ['cmx_target'] + [('_'.join(p) if p[0] else p[1]) + '_target'
                                  for p in itertools.product(surveys, programs)]
        for mask in masks:
            default_columns[mask] = 0
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
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
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
    program = Column(String(6), nullable=False)  # matterhorn: 8A
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
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "expid", "camera", unique=True),
                SchemaMixin.__table_args__)

    frameid = Column(Integer, primary_key=True, autoincrement=False)  # Arbitrary integer composed from expid + cameraid
    # frameid = Column(BigInteger, primary_key=True, autoincrement=True)
    night = Column(Integer, nullable=False, index=True)
    expid = Column(Integer, ForeignKey('exposure.expid'), nullable=False, index=True)
    tileid = Column(Integer, nullable=False, index=True)  # weird that this is not a foreign key
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
        log = get_logger()
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
        log = get_logger()
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
        log = get_logger()
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
                elif column.upper() in data.meta:
                    log.info("Obtaining '%s' from input data header.", column)
                    default_columns[column] = data.meta[column.upper()]
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
    spgrp = Column(String, nullable=False, index=True)  # cumulative
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
        log = get_logger()
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
                    log.info("Obtaining '%s' from input data table.", column)
                elif column.upper() in data.meta:
                    log.info("Obtaining '%s' from input data header.", column)
                    default_columns[column] = data.meta[column.upper()]
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
