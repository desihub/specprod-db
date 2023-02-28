# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from importlib import import_module
sys.path.insert(0, os.path.abspath('../py'))


# -- Project information -----------------------------------------------------

project = 'specprod-db'
python_project = 'specprodDB'
copyright = '2023, DESI Collaboration'
author = 'DESI Collaboration'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon'
]

# Configuration for intersphinx, copied from astropy.
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'scipy': ('https://docs.scipy.org/doc/scipy/', None),
    'matplotlib': ('https://matplotlib.org/stable/', None),
    'astropy': ('https://docs.astropy.org/en/stable/', None),
    'h5py': ('https://docs.h5py.org/en/latest/', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/14/', None),
    'desispec': ('https://desispec.readthedocs.io/en/latest/', None),
    'desiutil': ('https://desiutil.readthedocs.io/en/latest/', None),
    }

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build']

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.

__import__(python_project)
package = sys.modules[python_project]

# The short X.Y version.
version = package.__version__.split('-', 1)[0]
# The full version, including alpha/beta/rc tags.
release = package.__version__

# Include functions that begin with an underscore, e.g. _private().
napoleon_include_private_with_doc = True

# This value contains a list of modules to be mocked up. This is useful when
# some external dependencies are not met at build time and break the
# building process.
autodoc_mock_imports = []
for missing in ('desiutil', 'desispec', 'numpy', 'astropy', 'pytz', 'sqlalchemy'):
    try:
        foo = import_module(missing)
    except ImportError:
        autodoc_mock_imports.append(missing)

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
try:
    import sphinx_rtd_theme
    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
except ImportError:
    html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
