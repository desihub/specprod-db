# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.util
===============

Classes and functions for use by all database code.
"""
from sys import argv
from argparse import ArgumentParser
from datetime import datetime
from os.path import expanduser, exists, basename
import importlib.resources as ir
import numpy as np
from desitarget.targets import decode_targetid

from . import __version__ as specprodDB_version


_surveyid = {'cmx': 1, 'special': 2, 'sv1': 3, 'sv2': 4, 'sv3': 5, 'main': 6}
_decode_surveyid = dict([(v, k) for k, v in _surveyid.items()])
_programid = {'backup': 1, 'bright': 2, 'dark': 3, 'other': 4}
_decode_programid = dict([(v, k) for k, v in _programid.items()])
_spgrpid = {'1x_depth': 1, '4x_depth': 2, 'cumulative': 3, 'lowspeed': 4,
            'perexp': 5, 'pernight': 6, 'healpix': 7}
_decode_spgrpid = dict([(v, k) for k, v in _spgrpid.items()])


def cameraid(camera):
    """Converts `camera` (*e.g.* 'b0') to an integer in a simple but ultimately
    arbitrary way.

    Parameters
    ----------
    camera : :class:`str`
        Camera name.

    Returns
    -------
    :class:`int`
        An arbitrary integer, though in the range [0, 29].
    """
    return 'brz'.index(camera[0]) * 10 + int(camera[1])


def frameid(expid, camera):
    """Converts the pair `expid`, `camera` into an arbitrary integer
    suitable for use as a primary key.

    Parameters
    ----------
    expid : :class:`int`
        Exposure ID associated with the frame.
    camera : :class:`str`
        Camera name.

    Returns
    -------
    :class:`int`
        An arbitrary integer.
    """
    return 100*expid + cameraid(camera)


def surveyid(survey):
    """Converts `survey` (*e.g.* 'main') to an integer in a simple but ultimately
    arbitrary way.

    Parameters
    ----------
    survey : :class:`str`
        Survey name.

    Returns
    -------
    :class:`int`
        An arbitrary, small integer.
    """
    return _surveyid[survey]


def decode_surveyid(surveyid):
    """Converts `surveyid` to its corresponding name.

    Parameters
    ----------
    surveyid : :class:`int`
        Survey number

    Returns
    -------
    :class:`str`
        The name of the corresponding survey.
    """
    return _decode_surveyid[surveyid]


def programid(program):
    """Converts `program` (*e.g.* 'bright') to an integer in a simple but ultimately
    arbitrary way.

    Parameters
    ----------
    program : :class:`str`
        Program name.

    Returns
    -------
    :class:`int`
        An arbitrary, small integer.
    """
    return _programid[program]


def spgrpid(spgrp):
    """Converts `spgrp` (*e.g.* 'cumulative') to an integer in a simple but ultimately
    arbitrary way.

    Parameters
    ----------
    spgrp : :class:`str`
        SPGRP name.

    Returns
    -------
    :class:`int`
        An arbitrary, small integer.
    """
    return _spgrpid[spgrp]


def targetphotid(targetid, tileid, survey):
    """Convert inputs into an arbitrary large integer.

    Parameters
    ----------
    targetid : :class:`int`
        Standard ``TARGETID``.
    tileid : :class:`int`
        Standard ``TILEID``.
    survey : :class:`str`
        Survey name.

    Returns
    -------
    :class:`int`
        An arbitrary integer, which will be greater than :math:`2^64` but
        less than :math:`2^128`.
    """
    return (surveyid(survey) << 96) | (tileid << 64) | targetid


def decode_targetphotid(targetphotid):
    """Convert `id` into its components.

    Parameters
    ----------
    targetphotid : :class:`int`
        The 128-bit id.

    Returns
    -------
    :class:`tuple`
        A tuple of targetid, tileid and survey.
    """
    targetid = targetphotid & (2**64 - 1)
    t = targetphotid >> 64
    tileid = t & (2**32 - 1)
    survey = decode_surveyid(t >> 32)
    return (targetid, tileid, survey)


def zpixid(targetid, survey, program):
    """Convert inputs into an arbitrary large integer.

    Parameters
    ----------
    targetid : :class:`int`
        Standard ``TARGETID``.
    survey : :class:`str`
        Survey name.
    program : :class:`str`
        Program name.

    Returns
    -------
    :class:`int`
        An arbitrary integer, which will be greater than :math:`2^64` but
        less than :math:`2^128`.
    """
    return (programid(program) << 96) | (surveyid(survey) << 64) | targetid


def ztileid(targetid, spgrp, spgrpval, tileid):
    """Convert inputs into an arbitrary large integer.

    Parameters
    ----------
    targetid : :class:`int`
        Standard ``TARGETID``.
    spgrp : :class:`str`
        Tile grouping.
    spgrpval : :class:`int`
        Id within `spgrp`.
    tileid : :class:`int`
        Standard ``TILEID``.

    Returns
    -------
    :class:`int`
        An arbitrary integer, which will be greater than :math:`2^64` but
        less than :math:`2^128`.
    """
    spgrpid = (_spgrpid[spgrp] << 27) | spgrpval  # effective 32-bit integer
    return (spgrpid << 96) | (tileid << 64) | targetid


def fiberassignid(targetid, tileid, location):
    """Convert inputs into an arbitrary large integer.

    Parameters
    ----------
    targetid : :class:`int`
        Standard ``TARGETID``.
    tileid : :class:`int`
        Standard ``TILEID``.
    location : :class:`int`
        Location on the tile.

    Returns
    -------
    :class:`int`
        An arbitrary integer, which will be greater than :math:`2^64` but
        less than :math:`2^128`.
    """
    return (location << 96) | (tileid << 64) | targetid


def convert_dateobs(timestamp, tzinfo=None):
    """Convert a string `timestamp` into a :class:`datetime.datetime` object.

    Parameters
    ----------
    timestamp : :class:`str`
        Timestamp in string format.
    tzinfo : :class:`datetime.tzinfo`, optional
        If set, add time zone to the timestamp.

    Returns
    -------
    :class:`datetime.datetime`
        The converted `timestamp`.
    """
    x = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    if tzinfo is not None:
        x = x.replace(tzinfo=tzinfo)
    return x


def checkgzip(filename):
    """Check for existence of `filename`, with or without a ``.gz`` extension.

    Parameters
    ----------
    filename : :class:`str`
        Filename to check.

    Returns
    -------
    :class:`str`
        Path of existing file with or without ``.gz``.

    Raises
    ------
    :exc:`FileNotFoundError`
        If neither file type exists.
    """
    if exists(filename):
        return filename

    if filename.endswith('.gz'):
        altfilename = filename[0:-3]
    else:
        altfilename = filename + '.gz'

    if exists(altfilename):
        return altfilename
    else:
        raise FileNotFoundError(f'Neither {filename} nor {altfilename} could be found!')


def no_sky(catalog):
    """Identify objects in `catalog` that are not sky targets.

    Parameters
    ----------
    catalog : :class:`~astropy.table.Table`
        Any Table containing a ``TARGETID`` column.

    Returns
    -------
    :class:`numpy.ndarray`
        The indexes of rows that are not sky targets.
    """
    _, _, _, _, sky, _ = decode_targetid(catalog['TARGETID'])
    return np.where((sky == 0) & (catalog['TARGETID'] > 0))[0]


def parse_pgpass(hostname='specprod-db.desi.lbl.gov', username='desi_admin'):
    """Read a ``~/.pgpass`` file.

    Parameters
    ----------
    hostname : :class:`str`, optional
        Database hostname.
    username : :class:`str`, optional
        Database username.

    Returns
    -------
    :class:`str`
        A string suitable for creating a SQLAlchemy database engine, or None
        if no matching data was found.
    """
    fmt = "postgresql://{3}:{4}@{0}:{1}/{2}"
    try:
        with open(expanduser('~/.pgpass')) as p:
            lines = p.readlines()
    except FileNotFoundError:
        return None
    data = dict()
    for l in lines:
        d = l.strip().split(':')
        if d[0] in data:
            data[d[0]][d[3]] = fmt.format(*d)
        else:
            data[d[0]] = {d[3]: fmt.format(*d)}
    if hostname not in data:
        return None
    try:
        pgpass = data[hostname][username]
    except KeyError:
        return None
    return pgpass


def common_options(description):
    """Define a set of common command-line options.

    Individual command-line scripts will add additional options.

    Parameters
    ----------
    description : :class:`str`
        Define the description in the command-line help.

    Returns
    -------
    :class:`~argparse.ArgumentParser`
        An argument parser to which further arguments may be added.
    """
    prsr = ArgumentParser(description=description,
                          prog=basename(argv[0]))
    prsr.add_argument('-c', '--config', action='store', dest='config', metavar='FILE',
                      default=str(ir.files('specprodDB') / 'data' / 'load_specprod_db.ini'),
                      help="Override the default configuration file.")
    # prsr.add_argument('-d', '--data-release', action='store', dest='release',
    #                   default='edr', metavar='RELEASE',
    #                   help='Use data release RELEASE (default "%(default)s").')
    # prsr.add_argument('-f', '--filename', action='store', dest='dbfile',
    #                   default='specprod.db', metavar='FILE',
    #                   help='Store data in FILE (default "%(default)s").')
    # prsr.add_argument('-H', '--hostname', action='store', dest='hostname',
    #                   metavar='HOSTNAME', default='specprod-db.desi.lbl.gov',
    #                   help='If specified, connect to a PostgreSQL database on HOSTNAME (default "%(default)s").')
    # prsr.add_argument('-l', '--load', action='store', dest='load',
    #                   default='exposures', metavar='STAGE',
    #                   help='Load the set of files associated with STAGE (default "%(default)s").')
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
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s ' + specprodDB_version)
    return prsr
