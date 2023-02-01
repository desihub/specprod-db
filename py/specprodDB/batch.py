# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.batch
================

Generate batch scripts for loading the database.
"""
import os
from sys import argv
from argparse import ArgumentParser


template = """#!/bin/{shell}
#SBATCH --qos={qos}
#SBATCH --constraint={constraint}
#SBATCH --nodes=1
#SBATCH --time={time}
#SBATCH --job-name=load_specprod_db_{schema}_{stage}
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user={email}
module load specprod-db/main
{export}{schema}
srun -n 1 load_specprod_db {overwrite} \\
    --hostname {hostname} --username {username} \\
    --load {stage} --schema ${{SPECPROD}} \\
    --target-path {targetpath} --tiles-path {tilespath} ${{DESI_ROOT}}
"""


def get_options():
    """Parse command-line options.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed options.
    """
    prsr = ArgumentParser(description=("Prepare batch scripts for loading database."),
                          prog=os.path.basename(argv[0]))
    prsr.add_argument('-c', '--csh', action='store_true', dest='csh',
                      help='Use /bin/tcsh as the shell.')
    prsr.add_argument('-H', '--hostname', action='store', dest='hostname',
                      metavar='HOSTNAME', default='specprod-db.desi.lbl.gov',
                      help='If specified, connect to a PostgreSQL database on HOSTNAME (default %(default)s).')
    prsr.add_argument('-j', '--job-dir', action='store', dest='job_dir', metavar='DIR',
                      default=os.path.join(os.environ['HOME'], 'Documents', 'Jobs'),
                      help='Write batch job files to DIR (default %(default)s).')
    # prsr.add_argument('-o', '--overwrite', action='store_true', dest='overwrite',
    #                   help='Delete any existing file(s) before loading.')
    # prsr.add_argument('-r', '--rows', action='store', dest='chunksize',
    #                   type=int, default=50000, metavar='N',
    #                   help="Load N rows at a time (default %(default)s).")
    prsr.add_argument('-s', '--schema', action='store', dest='schema',
                      metavar='SCHEMA',
                      help='Set the schema name in the PostgreSQL database.')
    prsr.add_argument('-t', '--tiles-path', action='store', dest='tilespath', metavar='PATH',
                      help="Load fiberassign data from PATH.")
    prsr.add_argument('-T', '--target-path', action='store', dest='targetpath', metavar='PATH',
                      help="Load target photometry data from PATH.")
    prsr.add_argument('-U', '--username', action='store', dest='username',
                      metavar='USERNAME', default='desi_admin',
                      help="If specified, connect to a PostgreSQL database with USERNAME (default %(default)s).")
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Print extra information.')
    prsr.add_argument('email', metavar='EMAIL', help='Send batch messages to EMAIL.')
    # prsr.add_argument('datapath', metavar='DIR', help='Load the data in DIR.')
    options = prsr.parse_args()
    # if options.targetpath is None:
    #     options.targetpath = options.datapath
    return options


def prepare_template(options):
    """Convert command-line options to template inputs and format.

    Parameters
    ----------
    options : :class:`argparse.Namespace`
        The parsed options.

    Returns
    -------
    :class:`dict`
        A dictionary mapping file name to contents.
    """
    if options.csh:
        extension = 'csh'
        shell = 'tcsh'
        export = 'setenv SPECPROD '
    else:
        extension = 'sh'
        shell = 'bash'
        export = 'export SPECPROD='
    scripts = dict()
    if options.targetpath is None:
        targetpath = os.path.join(os.environ['DESI_ROOT'], 'public', 'edr')
    else:
        targetpath = options.targetpath
    if options.tilespath is None:
        tilespath = os.path.join(os.environ['DESI_TARGET'], 'fiberassign', 'tiles', 'trunk')
    else:
        tilespath = options.tilespath
    for stage in ('exposures', 'photometry', 'targetphot', 'target', 'redshift', 'fiberassign'):
        if stage == 'exposures':
            overwrite = '--overwrite'
        else:
            overwrite = ''
        script_name = 'load_specprod_db_{schema}_{stage}.{extension}'.format(schema=options.schema, stage=stage, extension=extension)
        t = {'shell': shell,
             'qos': 'regular',
             'constraint': 'cpu',
             'time': '12:00:00',
             'schema': options.schema,
             'stage': stage,
             'email': options.email,
             'export': export,
             'overwrite': overwrite,
             'hostname': options.hostname,
             'username': options.username,
             'targetpath': targetpath,
             'tilespath': tilespath}
        scripts[script_name] = template.format(**t)
    return scripts


def write_scripts(scripts, jobs):
    """Write scripts to job directory.

    Parameters
    ----------
    scripts : :class:`dict`
        A dictionary mapping file name to contents.
    jobs : :class:`str`
        Name of a directory to write to.
    """
    for s in scripts:
        path = os.path.join(jobs, s)
        with open(path, 'w') as j:
            j.write(scripts[s])


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = get_options()
    scripts = prepare_template(options)
    write_scripts(scripts, options.job_dir)
    return 0
