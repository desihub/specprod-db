# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.load
===============

Code for loading a spectroscopic production database. This includes both
targeting and redshift data.

Notes
-----
* Migrate to using separate ORM definitions for each release.
* Obtain as much imaging/targeting/fiberassign information from zcatalog files
  as possible. Some fiberassign columns may be in the EXP_FIBERMAP files.
"""
import os
import sys
import glob
from importlib import import_module
from configparser import ConfigParser

import numpy as np
from astropy import __version__ as astropy_version
from astropy.table import Table, join

from sqlalchemy import __version__ as sqlalchemy_version
from sqlalchemy import (create_engine, event, DDL, text)
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from desiutil import __version__ as desiutil_version
from desiutil.iers import freeze_iers
from desiutil.log import get_logger, DEBUG, INFO

from . import __version__ as specprodDB_version
from .util import (common_options, parse_pgpass, checkgzip, no_sky)


engine = None
dbSession = scoped_session(sessionmaker())
schemamodule = None


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
    rows = dbSession.query(schemamodule.Photometry.targetid,
                           schemamodule.Photometry.ls_id).order_by(schemamodule.Photometry.targetid).all()
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


def load_file(filepaths, tcls, hdu=1, row_filter=None, q3c=None, chunksize=50000,
              alternate_load=False):
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
        should return an array of indexes of "good" rows.
    q3c : :class:`str`, optional
        If set, create q3c index on the table, using the RA column
        named `q3c`.
    chunksize : :class:`int`, optional
        If set, load database `chunksize` rows at a time (default 50000).
    alternate_load : :class:`bool`, optional
        If ``True`` use an alternate loading scheme that may reduce memory use.

    Returns
    -------
    :class:`int`
        The grand total of rows loaded.
    """
    log = get_logger()
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
            good_rows = np.arange(len(data))
        else:
            good_rows = row_filter(data)
        if len(good_rows) == 0:
            log.info("Row filter removed all data rows, skipping %s.", filepath)
            continue
        log.info("Row filter applied on %s; %d rows remain.", tn, len(good_rows))
        if alternate_load:
            data = data[good_rows]
            finalrows = len(data)
        else:
            orm_objects = tcls.convert(data, row_index=good_rows)
            log.info("Converted data to ORM objects on %s.", tn)
            del data
            finalrows = len(orm_objects)
        n_chunks = finalrows//chunksize
        if finalrows % chunksize:
            n_chunks += 1
        for k in range(n_chunks):
            if alternate_load:
                data_chunk = tcls.convert(data[k*chunksize:(k+1)*chunksize])
            else:
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
        q3c_index(schemamodule.schemaname, tn, ra=q3c)
    return loaded_rows


def q3c_index(schema, table, ra='ra'):
    """Create a q3c index on a table.

    Parameters
    ----------
    schema : :class:`str`
        Name of the schema that `table` belongs to.
    table : :class:`str`
        Name of the table to index.
    ra : :class:`str`, optional
        If the RA, Dec columns are called something besides "ra" and "dec",
        set its name.  For example, ``ra='target_ra'``.
    """
    log = get_logger()
    q3c_sql = """CREATE INDEX IF NOT EXISTS ix_{table}_q3c_ang2ipix ON {schema}.{table} (q3c_ang2ipix({ra}, {dec}));
    CLUSTER {schema}.{table} USING ix_{table}_q3c_ang2ipix;
    ANALYZE {schema}.{table};
    """.format(ra=ra, dec=ra.lower().replace('ra', 'dec'),
               schema=schema, table=table)
    log.info("Creating q3c index on %s.%s.", schema, table)
    dbSession.execute(text(q3c_sql))
    log.info("Finished q3c index on %s.%s.", schema, table)
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
    log = get_logger()
    log.info("Loading version metadata.")
    version_table = Table()
    version_table['PACKAGE'] = np.array(['astropy', 'desiutil', 'lsdr9-photometry',
                                         'numpy', 'redshift', 'release', 'specprod',
                                         'specprod-db', 'sqlalchemy', 'tiles'])
    version_table['VERSION'] = np.array([astropy_version, desiutil_version, photometry,
                                         np.__version__, redshift, release, specprod,
                                         specprodDB_version, sqlalchemy_version, tiles])
    versions = schemamodule.Version.convert(version_table)
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
    :exc:`RuntimeError`
        If database connection details could not be found.
    """
    global engine, schemamodule
    log = get_logger()
    #
    # Schema creation
    #
    if schema:
        schemamodule = import_module(f"specprodDB.{schema}")
        # schemaname = schema
        # event.listen(Base.metadata, 'before_create', CreateSchema(schemamodule.schemaname))
        # if overwrite:
        #     event.listen(Base.metadata, 'before_create',
        #                  DDL('DROP SCHEMA IF EXISTS {0} CASCADE'.format(schemamodule.schemaname)))
        event.listen(schemamodule.Base.metadata, 'before_create',
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
        event.listen(schemamodule.Base.metadata, 'after_create', DDL(grant))
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
    # for tab in Base.metadata.tables.values():
    #     tab.schema = schemaname
    if overwrite:
        log.info("Begin creating tables.")
        schemamodule.Base.metadata.drop_all(engine)
        schemamodule.Base.metadata.create_all(engine)
        log.info("Finished creating tables.")
    #
    # Simplify access to ORM objects
    #
    for orm in ('Version', 'Photometry', 'Target', 'Tile', 'Exposure', 'Frame',
                'Fiberassign', 'Potential', 'Zpix', 'Ztile'):
        setattr(sys.modules[__name__], orm, schemamodule.__dict__[orm])
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
        # target_files = glob.glob(os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', f'targetphot-potential-*-{specprod}.fits'))
        target_files = glob.glob(os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', f'targetphot-potential-*.fits'))
    generate_primary = False
    if redshift_type == 'base' or redshift_type == 'patch':
        if redshift_type == 'base':
            redshift_dir = os.path.join(options.datapath, 'spectro', 'redux', specprod, 'zcatalog')
        else:
            redshift_dir = os.path.join(options.datapath, 'spectro', 'redux', specprod, 'zcatalog', redshift_version)
        if redshift_version == 'v2':
            redshift_dir = os.path.join(redshift_dir, 'zall')
        zpix_file = os.path.join(redshift_dir, f'zall-pix-{specprod}.fits')
        ztile_file = os.path.join(redshift_dir, f'zall-tilecumulative-{specprod}.fits')
        if not os.path.exists(zpix_file):
            log.warning("%s not found, will use individual survey-program files.")
            zpix_file = glob.glob(os.path.join(redshift_dir, f'zpix-*.fits'))
            generate_primary = True
        if not os.path.exists(ztile_file):
            log.warning("%s not found, will use individual survey-program files.")
            ztile_file = glob.glob(os.path.join(redshift_dir, f'ztile-*-cumulative.fits'))
            generate_primary = True
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
                              'tcls': schemamodule.Tile,
                              # 'hdu': 'TILE_COMPLETENESS',  # Ignored for CSV files.
                              'hdu': 'TILES',  # matterhorn++
                              'q3c': 'tilera',
                              'chunksize': chunksize
                              },
                             {'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'exposures-{specprod}.fits'),
                              'tcls': schemamodule.Exposure,
                              'hdu': 'EXPOSURES',
                              'q3c': 'tilera',
                              'chunksize': chunksize
                              },
                             {'filepaths': os.path.join(options.datapath, 'spectro', 'redux', specprod, f'exposures-{specprod}.fits'),
                              'tcls': schemamodule.Frame,
                              'hdu': 'FRAMES',
                              'chunksize': chunksize
                              }],
               #
               # The potential targets are supposed to include data for all targets.
               # In other words, every actual target is also a potential target.
               #
               'photometry': [{'filepaths': glob.glob(os.path.join(options.datapath, 'vac', release, 'lsdr9-photometry', specprod, photometry_version, 'potential-targets', 'tractorphot', 'tractorphot*.fits')),
                               'tcls': schemamodule.Photometry,
                               'hdu': 'TRACTORPHOT',
                               'chunksize': chunksize
                               }],
               #
               # This stage loads targets, and such photometry as they have, that did not
               # successfully match to a known LS DR9 object.
               #
               'targetphot': [{'filepaths': target_files,
                               'tcls': schemamodule.Photometry,
                               'hdu': 'TARGETPHOT',
                               'row_filter': deduplicate_targetid,
                               'q3c': 'ra',
                               'chunksize': chunksize
                               }],
               'target': [{'filepaths': target_files,
                           'tcls': schemamodule.Target,
                           'hdu': 'TARGETPHOT',
                           'chunksize': chunksize
                           }],
               'redshift': [{'filepaths': ztile_file,
                             'tcls': schemamodule.Ztile,
                             'hdu': 'ZCATALOG',
                             'row_filter': no_sky,
                             'chunksize': chunksize,
                             'alternate_load': True
                             }],
               'fiberassign': [{'filepaths': None,
                                'tcls': schemamodule.Fiberassign,
                                'hdu': 'FIBERASSIGN',
                                'row_filter': no_sky,
                                'q3c': 'target_ra',
                                'chunksize': chunksize
                                },
                               {'filepaths': None,
                                'tcls': schemamodule.Potential,
                                'hdu': 'POTENTIAL_ASSIGNMENTS',
                                'row_filter': no_sky,
                                'chunksize': chunksize
                                }]}
    if specprod != 'daily':
        loaders['redshift'].append({'filepaths': zpix_file,
                                    'tcls': schemamodule.Zpix,
                                    'hdu': 'ZCATALOG',
                                    'row_filter': no_sky,
                                    'chunksize': chunksize,
                                    'alternate_load': True
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
                                 for tileid in dbSession.query(schemamodule.Tile.tileid).order_by(schemamodule.Tile.tileid)]
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
    if options.load == 'redshift' and generate_primary:
        log.info("Generating global primary columns.")
    #
    # Clean up.
    #
    dbSession.close()
    engine.dispose()
    return 0
