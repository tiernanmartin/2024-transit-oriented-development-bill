
-- Rename geometry column 'geom'
ALTER TABLE uga RENAME COLUMN wkb_geometry TO geom;
ALTER TABLE parcels RENAME COLUMN wkb_geometry TO geom;

-- Set the spatial reference system for shapefiles
UPDATE zoning SET geom = ST_SetSRID(geom, 4326);

-- Convert to project CRS
ALTER TABLE zoning ALTER COLUMN geom TYPE geometry(MultiPolygon, 2926) USING ST_Transform(geom, 2926);
ALTER TABLE uga ALTER COLUMN geom TYPE geometry(MultiPolygon, 2926) USING ST_Transform(geom, 2926);
ALTER TABLE parcels ALTER COLUMN geom TYPE geometry(MultiPolygon, 2926) USING ST_Transform(geom, 2926);

-- Create parcel_pt with centroid geometry 
CREATE TABLE IF NOT EXISTS parcels_pt AS SELECT * FROM parcels;  
ALTER TABLE parcels_pt ADD COLUMN centroid geometry(Point, 2926); 
UPDATE parcels_pt SET centroid = ST_Centroid(geom); 
ALTER TABLE parcels_pt DROP COLUMN geom; 

-- Fix invalid geometries in zoning
UPDATE zoning 
SET geom =  ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);

-- Create spatial indices
CREATE INDEX idx_zoning_geom ON zoning USING GIST (geom);
CREATE INDEX idx_d_link ON zoning(d_link);
DROP INDEX IF EXISTS parcels_wkb_geometry_geom_idx;
CREATE INDEX idx_parcels_geom ON parcels USING GIST (geom);
CREATE INDEX idx_parcels_ogcfid ON parcels(ogc_fid);
CREATE INDEX idx_uga_geom ON uga USING GIST (geom);
DROP INDEX IF EXISTS transit_hct_geom_geom_idx;
CREATE INDEX idx_transit_hct_geom ON transit_hct USING GIST (geom);
CREATE INDEX idx_parcel_pt ON parcels_pt USING GIST (geom);
CREATE INDEX idx_parcels_pt_ogcfid ON parcels_pt(ogc_fid);
CREATE INDEX idx_parcel_pt_fips ON parcels_pt(fips_nr);
CREATE INDEX idx_parcel_pt_luc ON parcels_pt(landuse_cd);
CREATE INDEX zoning_details_idx ON zoning_details(d_link);
CREATE INDEX idx_land_use_codes ON land_use_codes(code);