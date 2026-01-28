--
-- SQL commands to apply coeff_patch patches, 2026-01-27.
--
--
-- Example SQL for moving loaded tables to the coeff_patch schema.
-- This is necessary to avoid name collisions on the indexes.
--
ALTER TABLE coeff_patch_fuji.zpixpatch RENAME TO fuji_zpixpatch;
ALTER TABLE coeff_patch_fuji.fuji_zpixpatch SET SCHEMA coeff_patch;
ALTER INDEX coeff_patch.zpixpatch_pkey RENAME TO fuji_zpixpatch_pkey;
ALTER INDEX coeff_patch.ix_zpixpatch_desiname RENAME TO ix_fuji_zpixpatch_desiname
--
-- SQL commands to test and apply patches.
--
-- guadalupe zpix
--
-- Count rows. All numbers should match.
--
SELECT COUNT(*) FROM guadalupe.zpix;
SELECT COUNT(*) FROM coeff_patch.guadalupe_zpixpatch;
SELECT COUNT(*) FROM guadalupe.zpix AS z
    JOIN coeff_patch.guadalupe_zpixpatch AS p ON z.id = p.id;
--
-- Test JOIN.
--
SELECT z.id,
    z.coeff_0 AS old_coeff_0, p.coeff_0 AS new_coeff_0,
    z.coeff_1 AS old_coeff_1, p.coeff_1 AS new_coeff_1,
    z.coeff_2 AS old_coeff_2, p.coeff_2 AS new_coeff_2,
    z.coeff_3 AS old_coeff_3, p.coeff_3 AS new_coeff_3,
    z.coeff_4 AS old_coeff_4, p.coeff_4 AS new_coeff_4,
    z.coeff_5 AS old_coeff_5, p.coeff_5 AS new_coeff_5,
    z.coeff_6 AS old_coeff_6, p.coeff_6 AS new_coeff_6,
    z.coeff_7 AS old_coeff_7, p.coeff_7 AS new_coeff_7,
    z.coeff_8 AS old_coeff_8, p.coeff_8 AS new_coeff_8,
    z.coeff_9 AS old_coeff_9, p.coeff_9 AS new_coeff_9
    FROM guadalupe.zpix AS z JOIN coeff_patch.guadalupe_zpixpatch AS p
    ON z.id = p.id LIMIT 100;
--
-- Apply the patch. The result should be UPDATE N, where N matches the counts above.
--
UPDATE guadalupe.zpix
    SET
        coeff_0 = p.coeff_0,
        coeff_1 = p.coeff_1,
        coeff_2 = p.coeff_2,
        coeff_3 = p.coeff_3,
        coeff_4 = p.coeff_4,
        coeff_5 = p.coeff_5,
        coeff_6 = p.coeff_6,
        coeff_7 = p.coeff_7,
        coeff_8 = p.coeff_8,
        coeff_9 = p.coeff_9
    FROM coeff_patch.guadalupe_zpixpatch AS p
    WHERE zpix.id = p.id;
--
-- Test that the values are the same after the patch.
--
SELECT z.id,
    z.coeff_0 - p.coeff_0 AS d_coeff_0,
    z.coeff_1 - p.coeff_1 AS d_coeff_1,
    z.coeff_2 - p.coeff_2 AS d_coeff_2,
    z.coeff_3 - p.coeff_3 AS d_coeff_3,
    z.coeff_4 - p.coeff_4 AS d_coeff_4,
    z.coeff_5 - p.coeff_5 AS d_coeff_5,
    z.coeff_6 - p.coeff_6 AS d_coeff_6,
    z.coeff_7 - p.coeff_7 AS d_coeff_7,
    z.coeff_8 - p.coeff_8 AS d_coeff_8,
    z.coeff_9 - p.coeff_9 AS d_coeff_9
    FROM guadalupe.zpix AS z JOIN coeff_patch.guadalupe_zpixpatch AS p
    ON z.id = p.id LIMIT 100;
--
-- guadalupe ztile
--
-- Count rows. All numbers should match.
--
SELECT COUNT(*) FROM guadalupe.ztile;
SELECT COUNT(*) FROM coeff_patch.guadalupe_ztilepatch;
SELECT COUNT(*) FROM guadalupe.ztile AS z
    JOIN coeff_patch.guadalupe_ztilepatch AS p ON z.id = p.id;
--
-- Test JOIN.
--
SELECT z.id,
    z.coeff_0 AS old_coeff_0, p.coeff_0 AS new_coeff_0,
    z.coeff_1 AS old_coeff_1, p.coeff_1 AS new_coeff_1,
    z.coeff_2 AS old_coeff_2, p.coeff_2 AS new_coeff_2,
    z.coeff_3 AS old_coeff_3, p.coeff_3 AS new_coeff_3,
    z.coeff_4 AS old_coeff_4, p.coeff_4 AS new_coeff_4,
    z.coeff_5 AS old_coeff_5, p.coeff_5 AS new_coeff_5,
    z.coeff_6 AS old_coeff_6, p.coeff_6 AS new_coeff_6,
    z.coeff_7 AS old_coeff_7, p.coeff_7 AS new_coeff_7,
    z.coeff_8 AS old_coeff_8, p.coeff_8 AS new_coeff_8,
    z.coeff_9 AS old_coeff_9, p.coeff_9 AS new_coeff_9
    FROM guadalupe.ztile AS z JOIN coeff_patch.guadalupe_ztilepatch AS p
    ON z.id = p.id LIMIT 100;
--
-- Apply the patch. The result should be UPDATE N, where N matches the counts above.
--
UPDATE guadalupe.ztile
    SET
        coeff_0 = p.coeff_0,
        coeff_1 = p.coeff_1,
        coeff_2 = p.coeff_2,
        coeff_3 = p.coeff_3,
        coeff_4 = p.coeff_4,
        coeff_5 = p.coeff_5,
        coeff_6 = p.coeff_6,
        coeff_7 = p.coeff_7,
        coeff_8 = p.coeff_8,
        coeff_9 = p.coeff_9
    FROM coeff_patch.guadalupe_ztilepatch AS p
    WHERE ztile.id = p.id;
--
-- Test that the values are the same after the patch.
--
SELECT z.id,
    z.coeff_0 - p.coeff_0 AS d_coeff_0,
    z.coeff_1 - p.coeff_1 AS d_coeff_1,
    z.coeff_2 - p.coeff_2 AS d_coeff_2,
    z.coeff_3 - p.coeff_3 AS d_coeff_3,
    z.coeff_4 - p.coeff_4 AS d_coeff_4,
    z.coeff_5 - p.coeff_5 AS d_coeff_5,
    z.coeff_6 - p.coeff_6 AS d_coeff_6,
    z.coeff_7 - p.coeff_7 AS d_coeff_7,
    z.coeff_8 - p.coeff_8 AS d_coeff_8,
    z.coeff_9 - p.coeff_9 AS d_coeff_9
    FROM guadalupe.ztile AS z JOIN coeff_patch.guadalupe_ztilepatch AS p
    ON z.id = p.id LIMIT 100;
--
-- Clean up.
--
VACUUM FULL VERBOSE ANALYZE guadalupe.zpix;
VACUUM FULL VERBOSE ANALYZE guadalupe.ztile;
--
-- All commands for other specprods can be reproduced by simple substitution,
-- e.g. guadalupe -> fuji, fuji -> iron.
--
-- Convert tables to Astro Data Lab naming convention. Only do this
-- after you have a full dump of the coeff_patch schema on tape!
--
DROP TABLE coeff_patch.guadalupe_zpixpatch;
DROP TABLE coeff_patch.guadalupe_ztilepatch;
ALTER TABLE coeff_patch.fuji_zpixpatch RENAME TO desi_edr_zpixpatch;
ALTER TABLE coeff_patch.fuji_ztilepatch RENAME TO desi_edr_ztilepatch;
ALTER TABLE coeff_patch.iron_zpixpatch RENAME TO desi_dr1_zpixpatch;
ALTER TABLE coeff_patch.iron_ztilepatch RENAME TO desi_dr1_ztilepatch;
--
ALTER INDEX coeff_patch.fuji_zpixpatch_pkey RENAME TO desi_edr_zpixpatch_pkey;
ALTER INDEX coeff_patch.ix_fuji_zpixpatch_desiname RENAME TO ix_desi_edr_zpixpatch_desiname;
ALTER INDEX coeff_patch.ix_fuji_zpixpatch_healpix RENAME TO ix_desi_edr_zpixpatch_healpix;
ALTER INDEX coeff_patch.ix_fuji_zpixpatch_program RENAME TO ix_desi_edr_zpixpatch_program;
ALTER INDEX coeff_patch.ix_fuji_zpixpatch_survey RENAME TO ix_desi_edr_zpixpatch_survey;
ALTER INDEX coeff_patch.ix_fuji_zpixpatch_targetid RENAME TO ix_desi_edr_zpixpatch_targetid;
ALTER INDEX coeff_patch.ix_fuji_zpixpatch_unique RENAME TO ix_desi_edr_zpixpatch_unique;
--
ALTER INDEX coeff_patch.fuji_ztilepatch_pkey RENAME TO desi_edr_ztilepatch_pkey;
ALTER INDEX coeff_patch.ix_fuji_ztilepatch_desiname RENAME TO ix_desi_edr_ztilepatch_desiname;
ALTER INDEX coeff_patch.ix_fuji_ztilepatch_spgrpval RENAME TO ix_desi_edr_ztilepatch_spgrpval;
ALTER INDEX coeff_patch.ix_fuji_ztilepatch_targetid RENAME TO ix_desi_edr_ztilepatch_targetid;
ALTER INDEX coeff_patch.ix_fuji_ztilepatch_tileid RENAME TO ix_desi_edr_ztilepatch_tileid;
ALTER INDEX coeff_patch.ix_fuji_ztilepatch_unique RENAME TO ix_desi_edr_ztilepatch_unique;
--
ALTER INDEX coeff_patch.iron_zpixpatch_pkey RENAME TO desi_dr1_zpixpatch_pkey;
ALTER INDEX coeff_patch.ix_iron_zpixpatch_desiname RENAME TO ix_desi_dr1_zpixpatch_desiname;
ALTER INDEX coeff_patch.ix_iron_zpixpatch_healpix RENAME TO ix_desi_dr1_zpixpatch_healpix;
ALTER INDEX coeff_patch.ix_iron_zpixpatch_program RENAME TO ix_desi_dr1_zpixpatch_program;
ALTER INDEX coeff_patch.ix_iron_zpixpatch_survey RENAME TO ix_desi_dr1_zpixpatch_survey;
ALTER INDEX coeff_patch.ix_iron_zpixpatch_targetid RENAME TO ix_desi_dr1_zpixpatch_targetid;
ALTER INDEX coeff_patch.ix_iron_zpixpatch_unique RENAME TO ix_desi_dr1_zpixpatch_unique;
--
ALTER INDEX coeff_patch.iron_ztilepatch_pkey RENAME TO desi_dr1_ztilepatch_pkey;
ALTER INDEX coeff_patch.ix_iron_ztilepatch_desiname RENAME TO ix_desi_dr1_ztilepatch_desiname;
ALTER INDEX coeff_patch.ix_iron_ztilepatch_spgrpval RENAME TO ix_desi_dr1_ztilepatch_spgrpval;
ALTER INDEX coeff_patch.ix_iron_ztilepatch_targetid RENAME TO ix_desi_dr1_ztilepatch_targeid;
ALTER INDEX coeff_patch.ix_iron_ztilepatch_tileid RENAME TO ix_desi_dr1_ztilepatch_tileid;
ALTER INDEX coeff_patch.ix_iron_ztilepatch_unique RENAME TO ix_desi_dr1_ztilepatch_unique;
--
-- And then make a *separate* pg_dump of coeff_patch.
--
-- Hint: convert a binary dump file to SQL:
--
--     pg_restore -f sql_file.sql binary.dump
--
