# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test specprodDB.batch.
"""
import os
import unittest
from unittest.mock import patch, mock_open, call
from ..batch import get_options, prepare_template, write_scripts
from .. import __version__ as specprod_db_version


class TestBatch(unittest.TestCase):
    """Test specprodDB.batch.
    """
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('sys.argv', ['prepare_batch_specprod_db', '--csh', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_get_options(self):
        """Test option parser.
        """
        options = get_options()
        self.assertTrue(options.csh)
        self.assertEqual(options.root, '/global/cfs/cdirs/desi')
        self.assertEqual(options.specprod, 'fuji')

    @patch('sys.argv', ['prepare_batch_specprod_db', '--csh', '--schema', 'fuji_test', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_prepare_template_csh(self):
        """Test conversion of options to scripts with csh.
        """
        with patch.dict('os.environ', {'DESI_ROOT': '/global/cfs/cdirs/desi', 'DESI_TARGET': '/global/cfs/cdirs/desi/target'}):
            options = get_options()
            scripts = prepare_template(options)
        self.assertIn('load_specprod_db_fuji_test_exposures.csh', scripts)

    @patch('sys.argv', ['prepare_batch_specprod_db', '--swap', '--schema', 'fuji_test', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_prepare_template_bash(self):
        """Test conversion of options to scripts with bash.
        """
        with patch.dict('os.environ', {'DESI_ROOT': '/global/cfs/cdirs/desi', 'DESI_TARGET': '/global/cfs/cdirs/desi/target'}):
            options = get_options()
            scripts = prepare_template(options)
        self.assertIn('load_specprod_db_fuji_test_exposures.sh', scripts)
        self.assertIn('module swap', scripts['load_specprod_db_fuji_test_exposures.sh'])

    @patch('sys.argv', ['prepare_batch_specprod_db', '--qos', 'bigmem', '--constraint', 'haswell', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_prepare_template_bash_qos(self):
        """Test conversion of options to scripts with bash and alternate qos/constraint.
        """
        expected = f"""#!/bin/bash
#SBATCH --qos=bigmem
#SBATCH --constraint=haswell
#SBATCH --nodes=1
#SBATCH --time=00:10:00
#SBATCH --job-name=load_specprod_db_fuji_exposures
#SBATCH --output=/home/test/Documents/Jobs/%x-%j.log
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=foo@example.com
module load specprod-db/{specprod_db_version}
export DESI_ROOT=/global/cfs/cdirs/desi
export SPECPROD=fuji
srun --ntasks=1 load_specprod_db --overwrite \\
    --load exposures --schema ${{SPECPROD}} ${{DESI_ROOT}}
[[ $? == 0 ]] && /bin/mv -v /home/test/Documents/Jobs/load_specprod_db_fuji_exposures.sh /home/test/Documents/Jobs/done
"""
        with patch('os.environ', {'HOME': '/home/test'}):
            options = get_options()
            scripts = prepare_template(options)
        self.assertIn('load_specprod_db_fuji_exposures.sh', scripts)
        self.assertEqual(scripts['load_specprod_db_fuji_exposures.sh'], expected)

    @patch('sys.argv', ['prepare_batch_specprod_db', '--schema', 'fuji_test', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_prepare_template_bash_schema(self):
        """Test conversion of options to scripts with bash and alternate schema name.
        """
        expected = f"""#!/bin/bash
#SBATCH --qos=regular
#SBATCH --constraint=cpu
#SBATCH --nodes=1
#SBATCH --time=04:00:00
#SBATCH --job-name=load_specprod_db_fuji_test_photometry
#SBATCH --output=/home/test/Documents/Jobs/%x-%j.log
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=foo@example.com
module load specprod-db/{specprod_db_version}
export DESI_ROOT=/global/cfs/cdirs/desi
export SPECPROD=fuji
srun --ntasks=1 load_specprod_db  \\
    --load photometry --schema fuji_test ${{DESI_ROOT}}
[[ $? == 0 ]] && /bin/mv -v /home/test/Documents/Jobs/load_specprod_db_fuji_test_photometry.sh /home/test/Documents/Jobs/done
"""
        with patch('os.environ', {'HOME': '/home/test'}):
            options = get_options()
            scripts = prepare_template(options)
        self.assertIn('load_specprod_db_fuji_test_photometry.sh', scripts)
        self.assertEqual(scripts['load_specprod_db_fuji_test_photometry.sh'], expected)

    @patch('sys.argv', ['prepare_batch_specprod_db', '--specprod-version', 'main', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_prepare_template_bash_version(self):
        """Test conversion of options to scripts with bash and alternate version name.
        """
        expected = f"""#!/bin/bash
#SBATCH --qos=regular
#SBATCH --constraint=cpu
#SBATCH --nodes=1
#SBATCH --time=04:00:00
#SBATCH --job-name=load_specprod_db_fuji_photometry
#SBATCH --output=/home/test/Documents/Jobs/%x-%j.log
#SBATCH --licenses=SCRATCH,cfs
#SBATCH --account=desi
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=foo@example.com
module load specprod-db/main
export DESI_ROOT=/global/cfs/cdirs/desi
export SPECPROD=fuji
srun --ntasks=1 load_specprod_db  \\
    --load photometry --schema ${{SPECPROD}} ${{DESI_ROOT}}
[[ $? == 0 ]] && /bin/mv -v /home/test/Documents/Jobs/load_specprod_db_fuji_photometry.sh /home/test/Documents/Jobs/done
"""
        with patch('os.environ', {'HOME': '/home/test'}):
            options = get_options()
            scripts = prepare_template(options)
        self.assertIn('load_specprod_db_fuji_photometry.sh', scripts)
        self.assertEqual(scripts['load_specprod_db_fuji_photometry.sh'], expected)

    @patch('sys.argv', ['prepare_batch_specprod_db', '--schema', 'fuji', 'foo@example.com', '/global/cfs/cdirs/desi', 'fuji'])
    def test_write_scripts(self):
        """Test conversion of options to scripts with bash.
        """
        options = get_options()
        # scripts = prepare_template(options)
        scripts = {'foo.sh': 'abcd', 'bar.sh': 'abcd'}
        m = mock_open()
        with patch('builtins.open', m) as mm:
            write_scripts(scripts, options.job_dir)
        m.assert_has_calls([call(os.path.join(os.environ['HOME'], 'Documents', 'Jobs', 'foo.sh'), 'w'),
                            call().__enter__(),
                            call().write('abcd'),
                            call().__exit__(None, None, None),
                            call(os.path.join(os.environ['HOME'], 'Documents', 'Jobs', 'bar.sh'), 'w'),
                            call().__enter__(),
                            call().write('abcd'),
                            call().__exit__(None, None, None),
                            ])
        handle = m()
        handle.write.assert_has_calls([call('abcd'), call('abcd')])
