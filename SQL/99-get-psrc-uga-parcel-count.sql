SELECT p.*
FROM parcels_pt p
JOIN uga u ON ST_Within(p.geom, u.geom)
WHERE p.fips_nr IN ('033','035','053','061');