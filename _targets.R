# LOAD PACKAGES ----

library(targets)
library(tidyverse)
library(DBI)
library(RPostgres)
library(glue)
library(sf)
library(janitor)
library(mapview)
library(readxl)
library(here)

# SET TARGET OPTIONS ----
tar_option_set(
  packages = c("tibble",
               "tidyverse",
               "DBI",
               "RPostgres",
               "glue",
               "sf",
               "janitor",
               "mapview",
               "readxl",
               "here")
)

# SOURCE R FUNCTIONS -----------------------------------------------------------

tar_source(files = here("R/functions.R"))

# PIPELINE PART: FILES ---------------------------------------------------------

pipeline_files <- 
  list(
    tar_target(parcels_file, 
               here("data/2023-07-01_parcels.geojson"),
               format = "file"),
    tar_target(transit_cr_file, 
               here("data/CR_stations/CR_stops.shp"),
               format = "file"),
    tar_target(transit_lr_file, 
               here("data/LR_stations/LR_stations.shp"),
               format = "file"),
    tar_target(transit_lr_alt_file, 
               here("data/sound-transit-lightrail.gpkg"),
               format = "file"),
    tar_target(transit_sc_file, 
               here("data/streetcar_stops/streetcar_stops.shp"),
               format = "file"),
    tar_target(transit_brt_file, 
               here("data/existingbrt_stops/existingbrt_stops.shp"),
               format = "file"),
    tar_target(zoning_details_file, 
               here("data/Puget_Sound_Zoning/seattle_zone_dictionary.csv"),
               format = "file"),
    tar_target(cities_file, 
               here("data/city_boundaries.geojson"),
               format = "file"),
    tar_target(uga_file, 
               here("data/uga.geojson"),
               format = "file"),
    tar_target(zoning_file, 
               here("data/Puget_Sound_Zoning/seattle_zone_data.shp"), 
               format = "file"),
    tar_target(landuse_codes_file, 
               here("data/Washington State Standard Assessment 2-Digit Codes.xlsx"), 
               format = "file")
    
    
  )


# PIPELINE PART: LOAD FILES -----------------------------------------------

pipeline_load_files <- 
  list(
    
    tar_target(transit_hct, 
               load_transit_hct(cr = transit_cr_file,
                                lr = transit_lr_file,
                                lr_alt = transit_lr_alt_file,
                                sc = transit_sc_file,
                                brt = transit_brt_file,
                                proj_crs = proj_crs)),
    tar_target(zoning_details, 
               load_zoning_details(zoning_details_file)),
    tar_target(cities, 
               load_uga(cities_file)),
    tar_target(uga, 
               load_uga(uga_file)),
    tar_target(zoning, 
               load_zoning(zoning_file)),
    tar_target(landuse_codes, 
               load_landuse_codes(landuse_codes_file)),
    tar_target(proj_crs, 2926L)
    
  )


# PIPELINE PART: POSTGRES ------------------------------------------------------

pipeline_postgres <- 
  list(
    tar_target(pg_write_parcels,
               write_to_db_ogr2ogr(filepath = parcels_file,
                              table_name = "parcels")),
    tar_target(pg_write_transit_hct,
               write_sf_to_db(x = transit_hct,
                              table_name = "transit_hct",
                              overwrite = TRUE)),
    tar_target(pg_write_zoning_details,
               write_to_db(x = zoning_details,
                           table_name = "zoning_details",
                           overwrite = TRUE)),
    tar_target(pg_write_cities,
               write_sf_to_db(x = cities,
                              table_name = "cities",
                              overwrite = TRUE)),
    tar_target(pg_write_uga,
               write_sf_to_db(x = uga,
                              table_name = "uga",
                              overwrite = TRUE)),
    tar_target(pg_write_zoning,
               write_sf_to_db(x = zoning,
                           table_name = "zoning",
                           overwrite = TRUE)),
    tar_target(pg_write_landuse_codes,
               write_to_db(x = landuse_codes,
                           table_name = "landuse_codes",
                           overwrite = TRUE))
  )


# MERGE PIPELINE PARTS ----------------------------------------------------

pipeline <- c(pipeline_files,
              pipeline_load_files,
              pipeline_postgres)


# RUN TARGET PIPELINE -----------------------------------------------------

pipeline
