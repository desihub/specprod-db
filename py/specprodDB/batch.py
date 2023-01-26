# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
specprodDB.batch
================

Generate batch scripts for loading the database.
"""
from .load import get_options


template = """
{shell}
#SBATCH --qos={qos}
#SBATCH --constraint={constraint}
#SBATCH --nodes=1
#SBATCH --time={time}
#SBATCH --job-name=load_specprod_db_{specprod}_{stage}
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=benjamin.weaver@noirlab.edu
module load specprod-db/main
setenv SPECPROD {SPECPROD}
srun -n 1 load_specprod_db {overwrite} --hostname {hostname} --username {username} --load {stage} --schema ${{SPECPROD}} --target-path /global/cfs/cdirs/desi/public/edr ${{DESI_ROOT}}
"""


def main():
    """Entry point for command-line script.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = get_options()
    print(options)
    return 0
