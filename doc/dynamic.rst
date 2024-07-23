====================================
Dynamic Loading for daily Reductions
====================================

Daily Database Loading Requirements
-----------------------------------

* The ``tiles-daily`` file should have some indications of which tiles need to
  be added, updated or deleted, *i.e.* ``INSERT``, ``UPDATE``, ``DELETE``.
  Hopefully deletes will be rare.

  - The tiles-daily file is only appended-to, old entries are not necessarily
    updated. See `desihub/desispec#2251`_ for more details.
  - There are many other potential issues with the :command:`desi_tsnr_afterburner`
    script. The issue linked above has details.

* Load redshifts from redrock files in ``tiles/cumulative``, rather than
  from the ``zcatalog`` summary files. HEALPix redshifts are not and probably never will
  be generated in daily reductions.
* Compute target and photometry data based on the objects being loaded rather
  than reading all at once. See further discussion below.
* For further discussion see `#13`_.

.. _`desihub/desispec#2251`: https://github.com/desihub/desispec/issues/2251
.. _`#13`: https://github.com/desihub/specprod-db/issues/13

Possible Load Scenario
----------------------

1. Read ``tiles-daily``, find changes. Update ``daily.tile``.
2. Find corresponding exposures. Update ``daily.exposure``, ``daily.frame``.
3. Find any *new* fiberassign files and obtain the list of new potentials (because potentials include observed targets).
4. Obtain the targeting and tractor data for new potentials. Update ``daily.photometry`` and ``daily.target``. Take care to check for existing entries.
5. Make sure to perform the equivalent of the ``targetphot`` stage, *i.e.* fill in photometric data from targeting data.
6. Update ``daily.fiberassign``, ``daily.potential``.
7. Read corresponding ``tiles/cumulative`` redshift files. Update ``daily.ztile``.

Automated Extraction of Targeting and Photometry
------------------------------------------------

* ``targetphot_onetile()`` in :command:`mpi-photometry` appears to work
  for both targeted and potential objects.
* ``tractorphot_onebrick()`` is similar.
* Both are thin wrappers on ``desispec.io.photo.gather_(tractor|target)phot()``.
* But the ``gather`` functions don't necessarily add ``DESINAME`` or fix ``NaN``
  values for ``PMRA`` or ``PMDEC``. Make sure inputs and outputs match.
* In the *database*, ``desiname`` is attached to the redshift tables, not the
  photometry or target tables. So it should be added at that stage, presumably.

Other Notes
-----------

- Plan for how to support fuji+guadalupe combined analysis.  May need to look
  into cross-schema views, or daughter tables that inherit from both schemas.
- Anticipate loading afterburners and VACs into the database.
- How do q3c indexes work with dynamic loading?
