BEGIN;

-- Join shape_length and shape_area
UPDATE parcels_walkshed
SET shape_area = p.area,
    shape_length = p.perimeter
FROM (
    SELECT ogc_fid, ST_Area(geom) as area, ST_Perimeter(geom) as perimeter
    FROM parcels
) as p
WHERE parcels_walkshed.ogc_fid = p.ogc_fid;

-- Drop the table if it already exists
DROP TABLE IF EXISTS parcels_walkshed_zoning;

CREATE TABLE parcels_walkshed_zoning AS
SELECT z.d_link AS zoning_d_link, 
	   z.gid AS zoning_id,
       p.is_within_lr_walkshed AS transit_within_lr_walkshed,
       p.is_within_sc_walkshed AS transit_within_sc_walkshed,
	   p.is_within_cr_walkshed AS transit_within_cr_walkshed,
       p.is_within_brt_walkshed AS transit_within_brt_walkshed,
       p.transit_walkshed_desc,
	   zd.juris AS zoning_juris,
	   zd.district AS zoning_district,
	   zd.zone_link AS zoning_zone_link,
	   zd.district_name AS zoning_district_name,
	   zd.legal_broad AS zoning_legal_broad,
	   zd.descript AS zoning_descript,
	   zd.planned_dev AS zoning_planned_dev,
	   zd.overlay AS zoning_overlay,
	   zd.allow_resi AS zoning_allow_resi,
	   zd.max_dwell_parcel AS zoning_max_dwell_parcel,
	   zd.min_lot_sq_ft AS zoning_min_lot_sq_ft,
	   zd.min_lot_sq_ft_unit AS zoning_min_lot_sq_ft_unit,
	   zd.max_units_acre AS zoning_max_units_acre,
	   zd.height_ft AS zoning_height_ft,
	   zd.height_stories AS zoning_height_stories,
	   zd.max_lot_coverage AS zoning_max_lot_coverage,
	   zd.max_far AS zoning_max_far,
	   zd.max_bldg_ftprt AS zoning_max_bldg_ftprt,
	   zd.side_sb_ft AS zoning_side_sb_ft,
	   zd.front_sb_ft AS zoning_front_sb_ft,
	   zd.rear_sb_ft AS zoning_rear_sb_ft,
	   zd.adu AS zoning_adu,
	   zd.min_parking AS zoning_min_parking,
       p.ogc_fid AS parcel_id,
       p.parcel_id_nr AS parcel_assessor_pid,
       p.situs_address AS parcel_address_street,
       p.situs_city_nm AS parcel_address_city,
       p.county_nm AS parcel_address_county,
       p.fips_nr AS parcel_address_county_fips,
       p.situs_zip_nr AS parcel_address_zip,
       p.value_land AS parcel_value_land,
       p.value_bldg AS parcel_value_bldg,
	   luc.category_short AS parcel_lu_code_category,
	   luc.use AS parcel_lu_code_use,
	   luc.code AS parcel_lu_code_number,
       p.shape_length AS parcel_length,
       p.shape_area AS parcel_area,
	   p.data_link AS parcel_data_link,
       p.geom
FROM parcels_walkshed p
JOIN zoning z ON ST_Within(p.geom, z.geom)
JOIN zoning_details zd ON z.d_link = zd.d_link
JOIN landuse_codes luc ON p.landuse_cd = luc.code;

COMMIT;