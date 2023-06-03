==========
Change Log
==========

1.2.0 (unreleased)
------------------

*Planned*: This release will correspond to loading the ``iron`` spectroscopic
production. A known change is that potential photometry and target data
will be split into several files instead of a monolithic file.

1.1.0 (unreleased)
------------------

* Corrections to the ``fuji`` schema in preparation for EDR (PR `#4`_).

.. _`#4`: https://github.com/desihub/specprod-db/pull/4

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
