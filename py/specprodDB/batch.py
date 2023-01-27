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


template = """
#!/bin/{shell}
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


def get_options(*args):
    """Parse command-line options.

    Parameters
    ----------
    args : iterable
        If arguments are passed, use them instead of ``sys.argv``.

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
    prsr.add_argument('-o', '--overwrite', action='store_true', dest='overwrite',
                      help='Delete any existing file(s) before loading.')
    # prsr.add_argument('-r', '--rows', action='store', dest='chunksize',
    #                   type=int, default=50000, metavar='N',
    #                   help="Load N rows at a time (default %(default)s).")
    prsr.add_argument('-s', '--schema', action='store', dest='schema',
                      metavar='SCHEMA',
                      help='Set the schema name in the PostgreSQL database.')
    prsr.add_argument('-t', '--tiles-path', action='store', dest='tilespath', metavar='PATH',
                      default=os.path.join(os.environ['DESI_TARGET'], 'fiberassign', 'tiles', 'trunk'),
                      help="Load fiberassign data from PATH (default %(default)s).")
    prsr.add_argument('-T', '--target-path', action='store', dest='targetpath', metavar='PATH',
                      default=os.path.join(os.environ['DESI_TARGET'], 'foo'),
                      help="Load target photometry data from PATH.")
    prsr.add_argument('-U', '--username', action='store', dest='username',
                      metavar='USERNAME', default='desi_admin',
                      help="If specified, connect to a PostgreSQL database with USERNAME (default %(default)s).")
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Print extra information.')
    prsr.add_argument('email', metavar='EMAIL', help='Send batch messages to EMAIL.')
    # prsr.add_argument('datapath', metavar='DIR', help='Load the data in DIR.')
    # if len(args) > 0:
    #     options = prsr.parse_args(args)
    # else:
    #     options = prsr.parse_args()
    options = prsr.parse_args()
    # if options.targetpath is None:
    #     options.targetpath = options.datapath
    return options


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = get_options()
    if options.csh:
        shell = 'tcsh'
        export = 'setenv SPECPROD '
    else:
        shell = 'bash'
        export = 'export SPECPROD='
    for stage in ('exposures', 'photometry', 'targetphot', 'target', 'redshift', 'fiberassign'):
        if stage == 'exposures':
            overwrite = '--overwrite'
        else:
            overwrite = ''
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
             'targetpath': options.targetpath,
             'tilespath': options.tilespath}
        print(template.format(**t))
    return 0
