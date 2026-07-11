# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.matterhorn
=====================

ORM definition for :envvar:`SPECPROD` = ``matterhorn``.
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


schemaname = 'matterhorn'
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
    release = Column(SmallInteger, nullable=False)  # zall-imaging
    brickid = Column(Integer, nullable=False)  # zall-imaging
    brickname = Column(String(8), nullable=False)  # zall-imaging
    brick_objid = Column(Integer, nullable=False)  # zall-imaging
    morphtype = Column(String(4), nullable=False)  # zall-imaging
    ra = Column(DOUBLE_PRECISION, nullable=False)  # zall
    ra_ivar = Column(REAL, nullable=False, default=-9999.0)
    dec = Column(DOUBLE_PRECISION, nullable=False)  # zall
    dec_ivar = Column(REAL, nullable=False, default=-9999.0)
    dchisq_psf = Column(REAL, nullable=False, default=-9999.0)
    dchisq_rex = Column(REAL, nullable=False, default=-9999.0)
    dchisq_dev = Column(REAL, nullable=False, default=-9999.0)
    dchisq_exp = Column(REAL, nullable=False, default=-9999.0)
    dchisq_ser = Column(REAL, nullable=False, default=-9999.0)
    ebv = Column(REAL, nullable=False)  # zall-imaging
    flux_g = Column(REAL, nullable=False)  # zall-imaging
    flux_r = Column(REAL, nullable=False)  # zall-imaging
    flux_z = Column(REAL, nullable=False)  # zall-imaging
    flux_ivar_g = Column(REAL, nullable=False)  # zall-imaging
    flux_ivar_r = Column(REAL, nullable=False)  # zall-imaging
    flux_ivar_z = Column(REAL, nullable=False)  # zall-imaging
    mw_transmission_g = Column(REAL, nullable=False, default=-9999.0)
    mw_transmission_r = Column(REAL, nullable=False, default=-9999.0)
    mw_transmission_z = Column(REAL, nullable=False, default=-9999.0)
    fracflux_g = Column(REAL, nullable=False, default=-9999.0)
    fracflux_r = Column(REAL, nullable=False, default=-9999.0)
    fracflux_z = Column(REAL, nullable=False, default=-9999.0)
    fracmasked_g = Column(REAL, nullable=False, default=-9999.0)
    fracmasked_r = Column(REAL, nullable=False, default=-9999.0)
    fracmasked_z = Column(REAL, nullable=False, default=-9999.0)
    fracin_g = Column(REAL, nullable=False, default=-9999.0)
    fracin_r = Column(REAL, nullable=False, default=-9999.0)
    fracin_z = Column(REAL, nullable=False, default=-9999.0)
    nobs_g = Column(SmallInteger, nullable=False, default=0)
    nobs_r = Column(SmallInteger, nullable=False, default=0)
    nobs_z = Column(SmallInteger, nullable=False, default=0)
    psfdepth_g = Column(REAL, nullable=False, default=-9999.0)
    psfdepth_r = Column(REAL, nullable=False, default=-9999.0)
    psfdepth_z = Column(REAL, nullable=False, default=-9999.0)
    galdepth_g = Column(REAL, nullable=False, default=-9999.0)
    galdepth_r = Column(REAL, nullable=False, default=-9999.0)
    galdepth_z = Column(REAL, nullable=False, default=-9999.0)
    flux_w1 = Column(REAL, nullable=False)  # zall-imaging
    flux_w2 = Column(REAL, nullable=False)  # zall-imaging
    flux_w3 = Column(REAL, nullable=False, default=-9999.0)
    flux_w4 = Column(REAL, nullable=False, default=-9999.0)
    flux_ivar_w1 = Column(REAL, nullable=False)  # zall-imaging
    flux_ivar_w2 = Column(REAL, nullable=False)  # zall-imaging
    flux_ivar_w3 = Column(REAL, nullable=False, default=-9999.0)
    flux_ivar_w4 = Column(REAL, nullable=False, default=-9999.0)
    mw_transmission_w1 = Column(REAL, nullable=False, default=-9999.0)
    mw_transmission_w2 = Column(REAL, nullable=False, default=-9999.0)
    mw_transmission_w3 = Column(REAL, nullable=False, default=-9999.0)
    mw_transmission_w4 = Column(REAL, nullable=False, default=-9999.0)
    allmask_g = Column(SmallInteger, nullable=False, default=0)
    allmask_r = Column(SmallInteger, nullable=False, default=0)
    allmask_z = Column(SmallInteger, nullable=False, default=0)
    fiberflux_g = Column(REAL, nullable=False)  # zall-imaging
    fiberflux_r = Column(REAL, nullable=False)  # zall-imaging
    fiberflux_z = Column(REAL, nullable=False)  # zall-imaging
    fibertotflux_g = Column(REAL, nullable=False)  # zall-imaging
    fibertotflux_r = Column(REAL, nullable=False)  # zall-imaging
    fibertotflux_z = Column(REAL, nullable=False)  # zall-imaging
    ref_epoch = Column(REAL, nullable=False)  # zall-imaging
    wisemask_w1 = Column(SmallInteger, nullable=False, default=0)
    wisemask_w2 = Column(SmallInteger, nullable=False, default=0)
    maskbits = Column(SmallInteger, nullable=False)  # zall-imaging
    # LC_...
    shape_r = Column(REAL, nullable=False)  # zall-imaging
    shape_e1 = Column(REAL, nullable=False)  # zall-imaging
    shape_e2 = Column(REAL, nullable=False)  # zall-imaging
    shape_r_ivar = Column(REAL, nullable=False, default=-9999.0)
    shape_e1_ivar = Column(REAL, nullable=False, default=-9999.0)
    shape_e2_ivar = Column(REAL, nullable=False, default=-9999.0)
    sersic = Column(REAL, nullable=False)  # zall-imaging
    sersic_ivar = Column(REAL, nullable=False, default=-9999.0)
    ref_id = Column(BigInteger, nullable=False)  # zall-imaging
    ref_cat = Column(String(2), nullable=False)  # zall-imaging
    gaia_phot_g_mean_mag = Column(REAL, nullable=False)  # zall-imaging
    gaia_phot_g_mean_flux_over_error = Column(REAL, nullable=False, default=-9999.0)
    gaia_phot_bp_mean_mag = Column(REAL, nullable=False)  # zall-imaging
    gaia_phot_bp_mean_flux_over_error = Column(REAL, nullable=False, default=-9999.0)
    gaia_phot_rp_mean_mag = Column(REAL, nullable=False)  # zall-imaging
    gaia_phot_rp_mean_flux_over_error = Column(REAL, nullable=False, default=-9999.0)
    gaia_phot_bp_rp_excess_factor = Column(REAL, nullable=False, default=-9999.0)
    gaia_duplicated_source = Column(Boolean, nullable=False, default=False)
    gaia_astrometric_sigma5d_max = Column(REAL, nullable=False, default=-9999.0)
    gaia_astrometric_params_solved = Column(SmallInteger, nullable=False, default=0)
    parallax = Column(REAL, nullable=False)  # zall-imaging
    parallax_ivar = Column(REAL, nullable=False, default=-9999.0)
    pmra = Column(REAL, nullable=False)  # zall-imaging
    pmra_ivar = Column(REAL, nullable=False, default=-9999.0)
    pmdec = Column(REAL, nullable=False)  # zall-imaging
    pmdec_ivar = Column(REAL, nullable=False, default=-9999.0)
    targetid = Column(BigInteger, primary_key=True, autoincrement=False)  # zall-imaging

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
        columns_present = list()
        for column in cls.__table__.columns:
            data_column = None
            if column.name == 'brick_objid' and 'BRICK_OBJID' not in data.colnames:
                data_column = data['OBJID'][row_index].tolist()
            elif column.name == 'morphtype' and 'MORPHTYPE' not in data.colnames:
                data_column = data['TYPE'][row_index].tolist()
            elif column.name == 'ls_id' and 'LS_ID' not in data.colnames:
                data_column = ((data[row_index]['RELEASE'].data.astype(np.int64) << 40) |
                               (data[row_index]['BRICKID'].data.astype(np.int64) << 16) |
                               (data[row_index]['BRICK_OBJID'].data.astype(np.int64))).tolist()
            elif column.name == 'gaia_astrometric_params_solved' and column.name.upper() in data.colnames and data[column.name.upper()].dtype.kind != 'i':
                data_column = data[column.name.upper()][row_index].data.astype(np.int16).tolist()
            elif column.name in expand_dchisq and 'DCHISQ' in data.colnames:
                j = expand_dchisq.index(column.name)
                data_column = data['DCHISQ'][row_index, j].tolist()
            else:
                try:
                    data_column = data[column.name.upper()][row_index].tolist()
                except KeyError:
                    pass
            if data_column is not None:
                data_columns.append(data_column)
                columns_present.append(column.name)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col, dat) for col, dat in zip(columns_present, row)]))) for row in data_rows]


class Target(SchemaMixin, Base):
    """Representation of the pure-desitarget quantities in the
    ``TARGETPHOT`` table in the targetphot files.
    """
    @declared_attr.directive
    def __table_args__(cls):
        return (Index(f'ix_{cls.__tablename__}_unique', "targetid", "survey", "tileid", unique=True),
                SchemaMixin.__table_args__)

    id = Column(Numeric(39), primary_key=True, autoincrement=False)
    targetid = Column(BigInteger, ForeignKey(f'{schemaname}.photometry.targetid'), nullable=False, index=True)  # fiberassign
    photsys = Column(String(1), nullable=False)  # zall-imaging
    subpriority = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    obsconditions = Column(BigInteger, nullable=False)  # zall-extra
    priority_init = Column(BigInteger, nullable=False)  # zall-extra
    numobs_init = Column(BigInteger, nullable=False)  # zall-extra
    hpxpixel = Column(BigInteger, nullable=False, default=-1, index=True)  # not available via zcatalog files
    cmx_target = Column(BigInteger, nullable=False, default=0)  # zall
    desi_target = Column(BigInteger, nullable=False, default=0)  # zall
    bgs_target = Column(BigInteger, nullable=False, default=0)  # zall
    mws_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv1_desi_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv1_bgs_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv1_mws_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv2_desi_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv2_bgs_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv2_mws_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv3_desi_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv3_bgs_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv3_mws_target = Column(BigInteger, nullable=False, default=0)  # zall
    scnd_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv1_scnd_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv2_scnd_target = Column(BigInteger, nullable=False, default=0)  # zall
    sv3_scnd_target = Column(BigInteger, nullable=False, default=0)  # zall
    survey = Column(String(7), nullable=False, index=True)  # zall
    program = Column(String(8), nullable=False, index=True)  # zall
    tileid = Column(Integer, ForeignKey(f'{schemaname}.tile.tileid'), nullable=False, index=True)

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
        default_columns = dict(hpxpixel=-1)
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
    program = Column(String(8), nullable=False)  # matterhorn: 8A
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
    tileid = Column(Integer, ForeignKey(f'{schemaname}.tile.tileid'), nullable=False, index=True)
    tilera = Column(DOUBLE_PRECISION, nullable=False)  # Calib exposures don't have RA, dec
    tiledec = Column(DOUBLE_PRECISION, nullable=False)
    date_obs = Column(DateTime(True), nullable=False)
    mjd = Column(DOUBLE_PRECISION, nullable=False)
    survey = Column(String(7), nullable=False)
    program = Column(String(8), nullable=False)  # matterhorn: 8A
    faprgrm = Column(String(16), nullable=False)
    faflavor = Column(String(19), nullable=False)
    exptime = Column(DOUBLE_PRECISION, nullable=False)
    efftime_spec = Column(DOUBLE_PRECISION, nullable=False)
    goaltime = Column(DOUBLE_PRECISION, nullable=False)
    goaltype = Column(String(7), nullable=False)  # This was increased from 6 to 7 to support 'unknown' in daily specprod.
    mintfrac = Column(DOUBLE_PRECISION, nullable=False)
    airmass = Column(REAL, nullable=False)
    ebv = Column(REAL, nullable=False)  # matterhorn: type E
    seeing_etc = Column(DOUBLE_PRECISION, nullable=False)  # matterhorn: all values below are type D
    efftime_etc = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_elg = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_qso = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_lrg = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_lya = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_bgs = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_gpbdark = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_gpbbright = Column(DOUBLE_PRECISION, nullable=False)
    tsnr2_gpbbackup = Column(DOUBLE_PRECISION, nullable=False)
    lrg_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    elg_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    bgs_efftime_bright = Column(DOUBLE_PRECISION, nullable=False)
    lya_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    gpb_efftime_dark = Column(DOUBLE_PRECISION, nullable=False)
    gpb_efftime_bright = Column(DOUBLE_PRECISION, nullable=False)
    gpb_efftime_backup = Column(DOUBLE_PRECISION, nullable=False)
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
    expid = Column(Integer, ForeignKey(f'{schemaname}.exposure.expid'), nullable=False, index=True)
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
    camera = Column(String(2), nullable=False, index=True)
    # 13 TSNR2_GPBDARK        E
    # 14 TSNR2_ELG            E
    # 15 TSNR2_GPBBRIGHT      E
    # 16 TSNR2_LYA            D
    # 17 TSNR2_BGS            E
    # 18 TSNR2_GPBBACKUP      E
    # 19 TSNR2_QSO            E
    # 20 TSNR2_LRG            E
    tsnr2_gpbdark = Column(REAL, nullable=False)  # matterhorn: all values below are type D
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
    tileid = Column(Integer, ForeignKey(f'{schemaname}.tile.tileid'), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey(f'{schemaname}.photometry.targetid'), nullable=False, index=True)
    petal_loc = Column(SmallInteger, nullable=False)  # zall
    device_loc = Column(Integer, nullable=False)  # zall-extra
    location = Column(Integer, nullable=False, index=True)  # zall-extra
    fiber = Column(Integer, nullable=False)  # zall
    fiberstatus = Column(Integer, nullable=False)  # zall via coadd_fiberstatus & 8
    target_ra = Column(DOUBLE_PRECISION, nullable=False)  # zall
    target_dec = Column(DOUBLE_PRECISION, nullable=False)  # zall
    pmra = Column(REAL, nullable=False)  # zall-imaging
    pmdec = Column(REAL, nullable=False)  # zall-imaging
    ref_epoch = Column(REAL, nullable=False)  # zall-imaging
    lambda_ref = Column(REAL, nullable=False)  # zall-extra
    fa_target = Column(BigInteger, nullable=False)  # zall-extra
    fa_type = Column(SmallInteger, nullable=False)  # zall-extra
    fiberassign_x = Column(REAL, nullable=False)  # zall
    fiberassign_y = Column(REAL, nullable=False)  # zall
    priority = Column(Integer, nullable=False)  # zall
    subpriority = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    parallax = Column(REAL, nullable=False)  # zall-imaging
    # plate_ra = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    # plate_dec = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra

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
    tileid = Column(Integer, ForeignKey(f'{schemaname}.tile.tileid'), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey(f'{schemaname}.photometry.targetid'), nullable=False, index=True)
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
    targetid = Column(BigInteger, ForeignKey(f'{schemaname}.photometry.targetid'), nullable=False, index=True)  # zall
    desiname = Column(String(22), nullable=False, index=True)  # zall
    survey = Column(String(7), nullable=False, index=True)  # zall
    program = Column(String(6), nullable=False, index=True)  # zall
    spgrp = Column(String(10), nullable=False, index=True)  # healpix by definition
    spgrpval = Column(Integer, nullable=False, index=True)  # zall-extra, same as healpix by definition
    uniqpix = Column(Integer, nullable=False, index=True)  # zall, renamed uniqpix, also in zall-extra
    z = Column(DOUBLE_PRECISION, index=True, nullable=False)  # zall, renamed z_best
    zerr = Column(REAL, nullable=False)  # zall, renamed zerr_best, type E
    zwarn = Column(Integer, index=True, nullable=False)  # zall, renamed zwarn_best, type J
    chi2 = Column(REAL, nullable=False)  # zall, renamed chi2_best, type E
    coeff_0 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_1 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_2 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_3 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_4 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_5 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_6 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_7 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_8 = Column(REAL, nullable=False)  # zall-extra, type E
    coeff_9 = Column(REAL, nullable=False)  # zall-extra, type E
    npixels = Column(Integer, nullable=False)  # zall-extra, type J
    spectype = Column(String(6), index=True, nullable=False)  # zall, renamed spectype_best
    subtype = Column(String(3), index=True, nullable=False)  # zall, renamed subtype_best, type 3A
    ncoeff = Column(SmallInteger, nullable=False)  # zall-extra, type I
    deltachi2 = Column(REAL, nullable=False)  # zall, renamed deltachi2_best, type E
    coadd_fiberstatus = Column(Integer, nullable=False)  # zall
    #
    # Skipping columns that are in other tables.
    #
    # These target bitmask columns are *not* the same as
    # in the zall-pix-specprod.fits file. They will be replaced
    # after the fact with values from the bitwise-or of
    # values in the target table.
    #
    cmx_target = Column(BigInteger, nullable=False)  # zall
    desi_target = Column(BigInteger, nullable=False)  # zall
    bgs_target = Column(BigInteger, nullable=False)  # zall
    mws_target = Column(BigInteger, nullable=False)  # zall
    scnd_target = Column(BigInteger, nullable=False)  # zall
    sv1_desi_target = Column(BigInteger, nullable=False)  # zall
    sv1_bgs_target = Column(BigInteger, nullable=False)  # zall
    sv1_mws_target = Column(BigInteger, nullable=False)  # zall
    sv1_scnd_target = Column(BigInteger, nullable=False)  # zall
    sv2_desi_target = Column(BigInteger, nullable=False)  # zall
    sv2_bgs_target = Column(BigInteger, nullable=False)  # zall
    sv2_mws_target = Column(BigInteger, nullable=False)  # zall
    sv2_scnd_target = Column(BigInteger, nullable=False)  # zall
    sv3_desi_target = Column(BigInteger, nullable=False)  # zall
    sv3_bgs_target = Column(BigInteger, nullable=False)  # zall
    sv3_mws_target = Column(BigInteger, nullable=False)  # zall
    sv3_scnd_target = Column(BigInteger, nullable=False)  # zall
    #
    # Skipping columns that are in other tables.
    #
    coadd_numexp = Column(SmallInteger, nullable=False)  # zall
    coadd_exptime = Column(REAL, nullable=False)  # zall
    coadd_numnight = Column(SmallInteger, nullable=False)  # zall
    coadd_numtile = Column(SmallInteger, nullable=False)  # zall
    mean_delta_x = Column(REAL, nullable=False)  # zall-extra
    rms_delta_x = Column(REAL, nullable=False)  # zall-extra
    mean_delta_y = Column(REAL, nullable=False)  # zall-extra
    rms_delta_y = Column(REAL, nullable=False)  # zall-extra
    mean_fiber_ra = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    std_fiber_ra = Column(REAL, nullable=False)  # zall-extra
    mean_fiber_dec = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    std_fiber_dec = Column(REAL, nullable=False)  # zall-extra
    mean_psf_to_fiber_specflux = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg = Column(REAL, nullable=False)  # zall-extra
    sv_nspec = Column(SmallInteger, nullable=False)  # zall
    sv_primary = Column(Boolean, nullable=False)  # zall
    main_nspec = Column(SmallInteger, nullable=False)  # zall
    main_primary = Column(Boolean, nullable=False)  # zall
    zcat_nspec = Column(SmallInteger, nullable=False)  # zall
    zcat_primary = Column(Boolean, nullable=False)  # zall
    # firstnight = Column(Integer, nullable=False)
    # lastnight = Column(Integer, nullable=False)
    min_mjd = Column(DOUBLE_PRECISION, nullable=False)  # zall
    mean_mjd = Column(DOUBLE_PRECISION, nullable=False)  # zall
    max_mjd = Column(DOUBLE_PRECISION, nullable=False)  # zall

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
        best_columns = {'z': 'Z_BEST', 'zerr': 'ZERR_BEST', 'zwarn': 'ZWARN_BEST',
                        'chi2': 'CHI2_BEST', 'spectype': 'SPECTYPE_BEST',
                        'subtype': 'SUBTYPE_BEST', 'deltachi2': 'DELTACHI2_BEST'}
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
            elif column.name == 'desiname' and column.name.upper() not in data.colnames:
                data_column = radec_to_desiname(data['TARGET_RA'][row_index], data['TARGET_DEC'][row_index]).tolist()
            elif column.name == 'spgrpval':
                data_column = data['UNIQPIX'][row_index].tolist()
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
            elif column.name in best_columns:
                data_column = data[best_columns[column.name]][row_index].tolist()
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
    targetphotid = Column(Numeric(39), ForeignKey(f"{schemaname}.target.id"), nullable=False, index=True)
    targetid = Column(BigInteger, ForeignKey(f'{schemaname}.photometry.targetid'), nullable=False, index=True)  # zall
    desiname = Column(String(22), nullable=False, index=True)  # zall
    survey = Column(String(7), nullable=False, index=True)  # zall
    program = Column(String(6), nullable=False, index=True)  # zall
    spgrp = Column(String, nullable=False, index=True)  # cumulative
    spgrpval = Column(Integer, nullable=False, index=True)  # zall
    z = Column(DOUBLE_PRECISION, index=True, nullable=False)  # zall, renamed to z_best
    zerr = Column(REAL, nullable=False)  # zall, renamed to zerr_best, type E
    zwarn = Column(Integer, index=True, nullable=False)  # zall, renamed to zwarn_best, type J
    chi2 = Column(REAL, nullable=False)  # zall, renamed to chi2_best, type E
    coeff_0 = Column(REAL, nullable=False)  # zall-extra
    coeff_1 = Column(REAL, nullable=False)  # zall-extra
    coeff_2 = Column(REAL, nullable=False)  # zall-extra
    coeff_3 = Column(REAL, nullable=False)  # zall-extra
    coeff_4 = Column(REAL, nullable=False)  # zall-extra
    coeff_5 = Column(REAL, nullable=False)  # zall-extra
    coeff_6 = Column(REAL, nullable=False)  # zall-extra
    coeff_7 = Column(REAL, nullable=False)  # zall-extra
    coeff_8 = Column(REAL, nullable=False)  # zall-extra
    coeff_9 = Column(REAL, nullable=False)  # zall-extra
    npixels = Column(Integer, nullable=False)  # zall-extra, type J
    spectype = Column(String(6), index=True, nullable=False)  # zall, renamed to spectype_best
    subtype = Column(String(3), index=True, nullable=False)  # zall, renamed to subtype_best, type 3A
    ncoeff = Column(SmallInteger, nullable=False)  # zall-extra, type I
    deltachi2 = Column(REAL, nullable=False)  # zall, renamed to deltachi2_best, type E
    coadd_fiberstatus = Column(Integer, nullable=False)  # zall
    #
    # Skipping columns that are in other tables.
    #
    tileid = Column(Integer, ForeignKey(f"{schemaname}.tile.tileid"), nullable=False, index=True)  # zall
    coadd_numexp = Column(SmallInteger, nullable=False)  # zall
    coadd_exptime = Column(REAL, nullable=False)  # zall
    coadd_numnight = Column(SmallInteger, nullable=False)  # zall
    coadd_numtile = Column(SmallInteger, nullable=False)  # zall
    mean_delta_x = Column(REAL, nullable=False)  # zall-extra
    rms_delta_x = Column(REAL, nullable=False)  # zall-extra
    mean_delta_y = Column(REAL, nullable=False)  # zall-extra
    rms_delta_y = Column(REAL, nullable=False)  # zall-extra
    mean_fiber_ra = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    std_fiber_ra = Column(REAL, nullable=False)  # zall-extra
    mean_fiber_dec = Column(DOUBLE_PRECISION, nullable=False)  # zall-extra
    std_fiber_dec = Column(REAL, nullable=False)  # zall-extra
    mean_psf_to_fiber_specflux = Column(REAL, nullable=False)  # zall-extra
    mean_fiber_x = Column(REAL, nullable=False)  # zall-extra
    mean_fiber_y = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg_b = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg_r = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg_z = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbdark = Column(REAL, nullable=False)  # zall-extra
    tsnr2_elg = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbright = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lya = Column(REAL, nullable=False)  # zall-extra
    tsnr2_bgs = Column(REAL, nullable=False)  # zall-extra
    tsnr2_gpbbackup = Column(REAL, nullable=False)  # zall-extra
    tsnr2_qso = Column(REAL, nullable=False)  # zall-extra
    tsnr2_lrg = Column(REAL, nullable=False)  # zall-extra
    sv_nspec = Column(SmallInteger, nullable=False)  # zall
    sv_primary = Column(Boolean, nullable=False)  # zall
    main_nspec = Column(SmallInteger, nullable=False)  # zall
    main_primary = Column(Boolean, nullable=False)  # zall
    zcat_nspec = Column(SmallInteger, nullable=False)  # zall
    zcat_primary = Column(Boolean, nullable=False)  # zall
    firstnight = Column(Integer, nullable=False)  # zall-extra
    lastnight = Column(Integer, nullable=False)  # zall
    min_mjd = Column(DOUBLE_PRECISION, nullable=False)  # zall
    mean_mjd = Column(DOUBLE_PRECISION, nullable=False)  # zall
    max_mjd = Column(DOUBLE_PRECISION, nullable=False)  # zall

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
        best_columns = {'z': 'Z_BEST', 'zerr': 'ZERR_BEST', 'zwarn': 'ZWARN_BEST',
                        'chi2': 'CHI2_BEST', 'spectype': 'SPECTYPE_BEST',
                        'subtype': 'SUBTYPE_BEST', 'deltachi2': 'DELTACHI2_BEST'}
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
            elif column.name == 'desiname' and column.name.upper not in data.colnames:
                data_column = radec_to_desiname(data['TARGET_RA'][row_index], data['TARGET_DEC'][row_index]).tolist()
            elif column.name in default_columns and column.name.upper() not in data.colnames:
                data_column = [default_columns[column.name]]*len(row_index)
            elif column.name in best_columns:
                data_column = data[best_columns[column.name]][row_index].tolist()
            elif column.name.startswith('coeff_'):
                coeff_index = int(column.name.split('_')[1])
                data_column = data['COEFF'][row_index, coeff_index].tolist()
            else:
                data_column = data[column.name.upper()][row_index].tolist()
            data_columns.append(data_column)
        data_rows = list(zip(*data_columns))
        return [cls(**(dict([(col.name, dat) for col, dat in zip(cls.__table__.columns, row)]))) for row in data_rows]
