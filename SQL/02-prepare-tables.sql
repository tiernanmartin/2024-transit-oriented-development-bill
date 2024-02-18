-- Time to complete: ~39 minutes
BEGIN;

-- Add geom column
ALTER TABLE uga ADD COLUMN geom geometry(MultiPolygon, 2926);
ALTER TABLE cities ADD COLUMN geom geometry(MultiPolygon, 2926);
ALTER TABLE parcels ADD COLUMN geom geometry(MultiPolygon, 4326);
ALTER TABLE transit_hct ADD COLUMN geom geometry(Point, 2926);
ALTER TABLE zoning ADD COLUMN geom geometry(MultiPolygon, 2926);

-- Convert wkb to native postgis geometry
UPDATE uga SET geom = ST_SetSRID(ST_GeomFromEWKB(wkb_geometry), 2926);
UPDATE cities SET geom = ST_SetSRID(ST_GeomFromEWKB(wkb_geometry), 2926);
UPDATE parcels SET geom = ST_SetSRID(ST_GeomFromEWKB(wkb_geometry), 4326);
UPDATE transit_hct SET geom = ST_SetSRID(ST_GeomFromEWKB(wkb_geometry), 2926);
UPDATE zoning SET geom = ST_SetSRID(ST_GeomFromEWKB(wkb_geometry), 2926);

-- Drop old wkb columns
ALTER TABLE uga DROP COLUMN wkb_geometry;
ALTER TABLE cities DROP COLUMN wkb_geometry;
ALTER TABLE parcels DROP COLUMN wkb_geometry;
ALTER TABLE transit_hct DROP COLUMN wkb_geometry;
ALTER TABLE zoning DROP COLUMN wkb_geometry;

COMMIT;

BEGIN;
-- Convert to project CRS
ALTER TABLE parcels ALTER COLUMN geom TYPE geometry(MultiPolygon, 2926) USING ST_Transform(geom, 2926);

-- Create parcel_pt with centroid geometry 
CREATE TABLE IF NOT EXISTS parcels_pt AS SELECT * FROM parcels;  
ALTER TABLE parcels_pt ADD COLUMN IF NOT EXISTS centroid geometry(Point, 2926); 
UPDATE parcels_pt SET centroid = ST_Centroid(geom) WHERE centroid IS NULL; 
ALTER TABLE parcels_pt DROP COLUMN geom;
ALTER TABLE parcels_pt RENAME COLUMN centroid TO geom;

-- Fix invalid geometries in zoning
UPDATE zoning 
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);

-- Create spatial indices 
CREATE INDEX IF NOT EXISTS idx_zoning_geom ON zoning USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_d_link ON zoning(d_link);
DROP INDEX IF EXISTS parcels_wkb_geometry_geom_idx;
CREATE INDEX IF NOT EXISTS idx_parcels_geom ON parcels USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_parcels_ogcfid ON parcels(ogc_fid);
CREATE INDEX IF NOT EXISTS idx_uga_geom ON uga USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_cities_geom ON cities USING GIST (geom);
DROP INDEX IF EXISTS cities_wkb_geometry_geom_idx;
DROP INDEX IF EXISTS transit_hct_wkb_geometry_geom_idx;
CREATE INDEX IF NOT EXISTS idx_transit_hct_geom ON transit_hct USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_parcel_pt ON parcels_pt USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_parcels_pt_ogcfid ON parcels_pt(ogc_fid);
CREATE INDEX IF NOT EXISTS idx_parcel_pt_fips ON parcels_pt(fips_nr);
CREATE INDEX IF NOT EXISTS idx_parcel_pt_luc ON parcels_pt(landuse_cd);
CREATE INDEX IF NOT EXISTS zoning_details_idx ON zoning_details(d_link);
CREATE INDEX IF NOT EXISTS idx_landuse_codes ON landuse_codes(code);

COMMIT;
