BEGIN;

-- Create transit_walksheds table with ST_Buffer process
DROP TABLE IF EXISTS transit_walksheds;

CREATE TABLE transit_walksheds AS
SELECT
  t.ogc_fid, t.mode, t.stop_name,
  ST_Buffer(t.geom, CASE
    WHEN t.mode = 'BRT' THEN 0.25 * 5280 -- BRT buffer in feet
    ELSE 0.5 * 5280 -- Default buffer in feet for other modes
  END) AS geom
FROM transit_hct t;

CREATE INDEX IF NOT EXISTS idx_transit_walksheds_geom ON transit_walksheds USING GIST (geom);

ANALYZE transit_walksheds;

COMMIT;

BEGIN;

-- Create zoning_walksheds
DROP TABLE IF EXISTS zoning_walksheds;

CREATE TABLE zoning_walksheds AS
SELECT
  z.fid, z.juris, z.zone, z.d_link,
  ST_Area(ST_Intersection(z.geom, t.geom)) AS zoning_district_area,
  ST_Intersection(z.geom, t.geom) AS geom
FROM zoning z
JOIN transit_walksheds t ON ST_Intersects(z.geom, t.geom);

CREATE INDEX IF NOT EXISTS idx_zoning_walksheds_geom ON zoning_walksheds USING GIST (geom);

COMMIT;