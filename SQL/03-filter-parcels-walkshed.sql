--Runtime: ~19 mins

-- Set memory for the operation
SET work_mem = '512MB';

-- See SQL/99-adjust-parallel-settings.sql for parallel settings use with this script

-- Run ANALYZE and VACUUM on the tables with geom columns
ANALYZE parcels_pt;
ANALYZE uga;
ANALYZE cities;
ANALYZE transit_hct;

-- Create the final table parcels_walkshed using CTEs 
DROP TABLE IF EXISTS parcels_walkshed;

CREATE TABLE parcels_walkshed AS
WITH 
parcels_psrc AS (
    SELECT * FROM parcels_pt WHERE fips_nr IN ('033','035','053','061') -- King, Kitsap, Pierce, Snohomish
),
parcels_uga AS (
    SELECT p.*
    FROM parcels_psrc p
    JOIN uga u ON ST_Within(p.geom, u.geom)
),
parcels_cities AS (
	SELECT p.*
	FROM parcels_uga p
	JOIN cities c ON ST_Within(p.geom, c.geom)
),
parcels_within_half_mile AS (
    SELECT p.*
    FROM parcels_cities p
    WHERE EXISTS (
        SELECT 1
        FROM transit_hct t
        WHERE ST_DWithin(p.geom, t.geom, 0.5 * 5280)
    )
),
transit_walkshed AS (
    SELECT p.ogc_fid AS parcel_id,
           t.stop_name,
           t.mode,
           ST_DWithin(p.geom, t.geom, 0.5 * 5280) AS within_half_mile,
           ST_DWithin(p.geom, t.geom, 0.25 * 5280) AS within_quarter_mile
    FROM parcels_within_half_mile p
    CROSS JOIN transit_hct t
),
walkshed_info AS (
    SELECT parcel_id,
           BOOL_OR(mode = 'LR' AND within_half_mile) AS is_within_lr_walkshed,
           BOOL_OR(mode = 'SC' AND within_half_mile) AS is_within_sc_walkshed,
		   BOOL_OR(mode = 'CR' AND within_half_mile) AS is_within_cr_walkshed,
           BOOL_OR(mode = 'BRT' AND within_quarter_mile) AS is_within_brt_walkshed,
           STRING_AGG(CASE WHEN within_half_mile OR within_quarter_mile THEN stop_name || ' (' || mode || ')' END, ', ') AS transit_walkshed_desc
    FROM transit_walkshed
    GROUP BY parcel_id
)
SELECT p.*, 
       w.is_within_lr_walkshed,
       w.is_within_sc_walkshed,
	   w.is_within_cr_walkshed,
       w.is_within_brt_walkshed,
       w.transit_walkshed_desc
FROM parcels_within_half_mile p
JOIN walkshed_info w ON p.ogc_fid = w.parcel_id
WHERE w.is_within_lr_walkshed OR w.is_within_sc_walkshed OR w.is_within_cr_walkshed OR w.is_within_brt_walkshed;

-- Create an index for the final table
CREATE INDEX IF NOT EXISTS idx_parcels_walkshed ON parcels_walkshed USING GIST (geom);