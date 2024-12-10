==========
Change Log
==========

1.4.0 (unreleased)
------------------

*Planned*: Support loading ongoing daily reductions, in particular, updates
to tiles already in the database.

1.3.0 (2024-12-10)
------------------

* This release corresponds to loading the ``loa`` spectroscopic production
  in preparation for DR2 (PR `#17`_).

.. _`#17`: https://github.com/desihub/specprod-db/pull/17

1.2.1 (2024-10-07)
------------------

* Finalize patches of ``tiles-daily`` and ``exposures-daily`` files (PR `#16`_).

.. _`#16`: https://github.com/desihub/specprod-db/pull/16

1.2.0 (2024-09-26)
------------------

This release corresponds to loading the ``guadalupe`` and ``iron`` spectroscopic
productions in preparation for DR1. See also PR `#12`_ and `#14`_. Some specific changes:

* Photometry and target data are split among several files instead of a monolithic file.
* Add ``DESINAME`` column to redshift tables.
* Move version configuration to a configuration file.
* Default values for targeting bits when they are absent from target files.
* Support SQLAlchemy 2.
* ORM objects know how to "load themselves".
* Support for tile-based loading, needed to load ``daily`` reductions.

.. _`#12`: https://github.com/desihub/specprod-db/pull/12
.. _`#14`: https://github.com/desihub/specprod-db/pull/14

1.1.0 (2023-06-09)
------------------

* Corrections to the ``fuji`` schema in preparation for EDR (PR `#4`_, `#8`_).

.. _`#4`: https://github.com/desihub/specprod-db/pull/4
.. _`#8`: https://github.com/desihub/specprod-db/pull/8

1.0.0 (2023-02-28)
------------------

* This release was used for loading ``fuji`` and ``guadalupe`` with
  all data files tagged.
* Set the final tagged version of tiles files (PR `#1`_).

.. _`#1`: https://github.com/desihub/specprod-db/pull/1

0.9.0 (2023-02-08)
------------------

* Initial tag. Version number reflects the fact that extensive development
  was performed in the desispec_ package prior to moving code to this
  package.
* A test-load of ``fuji`` and ``guadalupe`` are visible on ``nerscdb03.nersc.gov``.
  This test-load uses the ``trunk`` version of fiberassign files, but otherwise
  should be identical to a final release version for those to specprods.

.. _desispec: https://github.com/desihub/desispec
