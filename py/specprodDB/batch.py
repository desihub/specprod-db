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
from astropy.table import Table
from . import __version__ as specprod_db_version


template = """#!/bin/{shell}
#SBATCH --qos={qos}
#SBATCH --constraint={constraint}
#SBATCH --nodes=1
#SBATCH --time={time}
#SBATCH --job-name=load_specprod_db_{script_schema}_{stage}
#SBATCH --output={job_dir}/%x-%j.log
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user={email}
module swap specprod-db/{load_version}
{export_root}
{export_specprod}
srun --ntasks=1 load_specprod_db {overwrite} \\
    --load {stage} --schema {schema} ${{DESI_ROOT}}
{save_status}
{move_script}
exit ${{load_status}}
"""


tile_template = """#!/bin/{shell}
#SBATCH --qos={qos}
#SBATCH --constraint={constraint}
#SBATCH --nodes=1
#SBATCH --time={time}
#SBATCH --job-name=load_specprod_tile_{script_schema}_{tileid:d}
#SBATCH --output={job_dir}/%x-%j.log
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user={email}
module swap specprod-db/{load_version}
{export_specprod}
srun --ntasks=1 load_specprod_tile {exposures_file} {tiles_file} \\
     --schema ${{SPECPROD}} {overwrite} --verbose {tileid:d}
{save_status}
{move_script}
exit ${{load_status}}
"""


times = {'exposures': '00:10:00',
         'photometry': '04:00:00'}


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
    prsr.add_argument('-C', '--constraint', action='store', dest='constraint',
                      metavar='CONSTRAINT', default='cpu',
                      help='Run jobs with CONSTRAINT (default "%(default)s").')
    prsr.add_argument('-e', '--exposures-file', action='store', dest='exposures_file', metavar='FILE',
                      help='Override the top-level exposures file associated with a specprod.')
    # prsr.add_argument('-H', '--hostname', action='store', dest='hostname',
    #                   metavar='HOSTNAME', default='specprod-db.desi.lbl.gov',
    #                   help='If specified, connect to a PostgreSQL database on HOSTNAME (default "%(default)s").')
    prsr.add_argument('-j', '--job-dir', action='store', dest='job_dir', metavar='DIR',
                      default=os.path.join(os.environ['HOME'], 'Documents', 'Jobs'),
                      help='Write batch job files to DIR (default "%(default)s").')
    prsr.add_argument('-p', '--patch-tiles', action='store_true', dest='patch_tiles',
                      help='If --tiles-file is set, --patch-tiles means the file is a patched version of the actual tile file.')
    # prsr.add_argument('-o', '--overwrite', action='store_true', dest='overwrite',
    #                   help='Delete any existing file(s) before loading.')
    prsr.add_argument('-q', '--qos', action='store', dest='qos',
                      metavar='QUEUE', default='regular',
                      help='Run jobs on QUEUE (default "%(default)s").')
    # prsr.add_argument('-r', '--rows', action='store', dest='chunksize',
    #                   type=int, default=50000, metavar='N',
    #                   help="Load N rows at a time (default %(default)s).")
    prsr.add_argument('-s', '--schema', action='store', dest='schema',
                      metavar='SCHEMA', default='${SPECPROD}',
                      help='Set the schema name in the PostgreSQL database (default "%(default)s").')
    # prsr.add_argument('-S', '--swap', action='store_true', dest='swap',
    #                   help='Perform "module swap" instead of "module load".')
    prsr.add_argument('-t', '--tiles-file', action='store', dest='tiles_file', metavar='FILE',
                      help='Override the top-level tiles file associated with a specprod.')
    # prsr.add_argument('-t', '--tiles-path', action='store', dest='tilespath', metavar='PATH',
    #                   help="Load fiberassign data from PATH.")
    # prsr.add_argument('-T', '--target-path', action='store', dest='targetpath', metavar='PATH',
    #                   help="Load target photometry data from PATH.")
    # prsr.add_argument('-U', '--username', action='store', dest='username',
    #                   metavar='USERNAME', default='desi_admin',
    #                   help='If specified, connect to a PostgreSQL database with USERNAME (default "%(default)s").')
    prsr.add_argument('-v', '--specprod-version', metavar='VERSION', dest='specprod_version',
                      help='Set the specprod-db version to VERSION.')
    prsr.add_argument('email', metavar='EMAIL', help='Send batch messages to EMAIL.')
    prsr.add_argument('root', metavar='DIR', help='Load the data in DIR.')
    prsr.add_argument('specprod', metavar='SPECPROD', help='Set the value of SPECPROD, which may be different from the schema name.')
    options = prsr.parse_args()
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
        export_root = f'setenv DESI_ROOT {options.root}'
        export_specprod = f'setenv SPECPROD {options.specprod}'
        save_status = 'set load_status = ${status}'
        move_script = 'if ( ${{load_status}} == 0 ) /bin/mv -v {job_dir}/{script_name} {job_dir}/done'
    else:
        extension = 'sh'
        shell = 'bash'
        export_root = f'export DESI_ROOT={options.root}'
        export_specprod = f'export SPECPROD={options.specprod}'
        save_status = 'load_status=$?'
        move_script = '[[ ${{load_status}} == 0 ]] && /bin/mv -v {job_dir}/{script_name} {job_dir}/done'
    scripts = dict()
    if options.specprod_version is None:
        load_version = specprod_db_version
    else:
        load_version = options.specprod_version
    if options.schema == '${SPECPROD}':
        script_schema = options.specprod
    else:
        script_schema = options.schema
    if options.tiles_file is None:
        for stage in ('exposures', 'photometry', 'targetphot', 'target', 'redshift', 'fiberassign'):
            if stage == 'exposures':
                overwrite = '--overwrite'
            else:
                overwrite = ''
            try:
                wall_time = times[stage]
            except KeyError:
                wall_time = '12:00:00'
            script_name = 'load_specprod_db_{schema}_{stage}.{extension}'.format(schema=script_schema, stage=stage, extension=extension)
            t = {'shell': shell,
                 'qos': options.qos,
                 'constraint': options.constraint,
                 'time': wall_time,
                 'script_schema': script_schema,
                 'load_version': load_version,
                 'schema': options.schema,
                 'stage': stage,
                 'job_dir': options.job_dir,
                 'email': options.email,
                 'load_version': load_version,
                 'export_root': export_root,
                 'export_specprod': export_specprod,
                 'overwrite': overwrite,
                 'save_status': save_status,
                 'move_script': move_script.format(job_dir=options.job_dir, script_name=script_name)}
            scripts[script_name] = template.format(**t)
    else:
        if options.tiles_file.endswith('.csv'):
            tiles_table = Table.read(options.tiles_file, format='ascii.csv')
        else:
            tiles_table = Table.read(options.tiles_file, format='fits')
        good_tiles = ((tiles_table['LASTNIGHT'] >= 20201214) &
                      (tiles_table['EFFTIME_SPEC'] > 0))
        for tile_index, tileid in enumerate(tiles_table[good_tiles]['TILEID'].tolist()):
            if tile_index == 0:
                overwrite = '--overwrite'
            elif tile_index == len(tiles_table[good_tiles]) - 1:
                overwrite = '--primary'
            else:
                overwrite = ''
            if overwrite == '--primary':
                wall_time = '6:00:00'
            else:
                wall_time = '1:00:00'
            if options.patch_tiles:
                tiles_file = '--tiles-file {0}'.format(options.tiles_file)
                if options.exposures_file is None:
                    exposures_file = ''
                else:
                    exposures_file = '--exposures-file {0}'.format(options.exposures_file)
            else:
                tiles_file = ''
                exposures_file = ''
            script_name = 'load_specprod_tile_{schema}_{tileid:d}.{extension}'.format(schema=script_schema, tileid=tileid, extension=extension)
            t = {'shell': shell,
                 'qos': options.qos,
                 'constraint': options.constraint,
                 'time': wall_time,
                 'script_schema': script_schema,
                 'load_version': load_version,
                 'schema': options.schema,
                 'tileid': tileid,
                 'job_dir': options.job_dir,
                 'email': options.email,
                 'load_version': load_version,
                 'export_root': export_root,
                 'export_specprod': export_specprod,
                 'tiles_file': tiles_file,
                 'exposures_file': exposures_file,
                 'overwrite': overwrite,
                 'save_status': save_status,
                 'move_script': move_script.format(job_dir=options.job_dir, script_name=script_name)}
            scripts[script_name] = tile_template.format(**t)
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
