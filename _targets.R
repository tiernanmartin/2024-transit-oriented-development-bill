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
library(scales)
library(tidycensus)
library(viridisLite)
library(gt)

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
               "here",
               "scales",
               "tidycensus",
               "viridisLite",
               "gt")
)

# SOURCE R FUNCTIONS -----------------------------------------------------------

tar_source(files = here("R/functions.R"))

# PIPELINE PART: FILES ---------------------------------------------------------

pipeline_files <- 
  list(
    tar_target(postcensal_housing_file,
               here("data/ofm_april1_postcensal_estimates_housing_1980_1990-present.xlsx"),
               format = "file"),
    tar_target(postcensal_pop_file,
               here("data/ofm_april1_postcensal_estimates_pop_1960-present.xlsx"),
               format = "file"),
    tar_target(sql_02_file, 
               here("SQL/02-prepare-tables.sql"),
               format = "file"),
    tar_target(sql_03_file, 
               here("SQL/03-filter-parcels-walkshed.sql"),
               format = "file"),
    tar_target(sql_04_file, 
               here("SQL/04-join-zoning.sql"),
               format = "file"),
    tar_target(sql_05_file, 
               here("SQL/05-create-zoning-walksheds.sql"),
               format = "file"),
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
    tar_target(proj_crs, 2926L),
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
               load_cities(cities_file, proj_crs = proj_crs)),
    tar_target(uga, 
               load_uga(uga_file, proj_crs = proj_crs)),
    tar_target(zoning,
               load_zoning(zoning_file, proj_crs = proj_crs)),
    tar_target(landuse_codes, 
               load_landuse_codes(landuse_codes_file))
    
  )


# PIPELINE PART: LOAD INTO POSTGRES ------------------------------------------------------

pipeline_load_into_pg <- 
  list( 
    tar_target(pg_write_parcels,
               write_to_db_ogr2ogr(filepath = parcels_file,
                              table_name = "parcels"),
               cue = tar_cue("never")),
    tar_target(pg_write_transit_hct,
               write_sf_to_db(x = transit_hct,
                              table_name = "transit_hct",
                              overwrite = TRUE),
               cue = tar_cue("never")),
    tar_target(pg_write_zoning_details,
               write_to_db(x = zoning_details,
                           table_name = "zoning_details",
                           overwrite = TRUE),
               cue = tar_cue("never")),
    tar_target(pg_write_cities,
               write_sf_to_db(x = cities,
                              table_name = "cities",
                              overwrite = TRUE),
               cue = tar_cue("never")),
    tar_target(pg_write_uga,
               write_sf_to_db(x = uga,
                              table_name = "uga",
                              overwrite = TRUE),
               cue = tar_cue("never")),
    tar_target(pg_write_zoning,
               write_sf_to_db(x = zoning,
                           table_name = "zoning",
                           overwrite = TRUE),
               cue = tar_cue("never")),
    tar_target(pg_write_landuse_codes,
               write_to_db(x = landuse_codes,
                           table_name = "landuse_codes",
                           overwrite = TRUE),
               cue = tar_cue("never"))
  )



# PIPELINE PART: CREATE POSTGRES TABLES -----------------------------------

pipeline_create_pg_tables <- list(
  tar_target(pg_02_prepare_tables,
             run_sql_query(filepath = sql_02_file,
                           table_name = "parcels_pt",
                           target_dependencies = list(pg_write_parcels,
                                                      pg_write_landuse_codes,
                                                      pg_write_zoning,
                                                      pg_write_uga,
                                                      pg_write_cities,
                                                      pg_write_zoning_details,
                                                      pg_write_transit_hct)),
             cue = tar_cue("never")),
  tar_target(pg_03_filter_parcels_walkshed,
             run_sql_query(filepath = sql_03_file,
                           table_name = "parcels_walkshed",
                           target_dependencies = list(pg_02_prepare_tables)),
             cue = tar_cue("never")),
  tar_target(pg_04_join_zoning,
             run_sql_query(filepath = sql_04_file,
                           table_name = "parcels_walkshed_zoning",
                           target_dependencies = list(pg_03_filter_parcels_walkshed)),
             cue = tar_cue("never")),
  tar_target(pg_05_create_transit_walksheds,
             run_sql_query(filepath = sql_05_file,
                           table_name = "transit_walksheds",
                           target_dependencies = list(pg_04_join_zoning)),
             cue = tar_cue("never"))
  
)



# PIPELINE PART: CREATE ANALYSIS DATASET ----------------------------------

pipeline_analysis <- list(
  tar_target(analysis_transit_walksheds,
             make_analysis_transit_walksheds(target_dependencies = list(pg_05_create_transit_walksheds))),
  tar_target(analysis_parcels_revised,
             make_parcels_revised(target_dependencies = list(pg_05_create_transit_walksheds))),
  tar_target(analysis_excluded_landuse_categories,
             make_excluded_landuse_categories(target_dependencies = list(pg_05_create_transit_walksheds))),
  tar_target(analysis_parcels_ndc,
             make_parcels_ndc(p = analysis_parcels_revised,
                              excl_lu = analysis_excluded_landuse_categories)),
  tar_target(analysis_study_group,
             make_analysis_study_groups(analysis_parcels_ndc))
)


# PIPELINE PART: PUGET SOUND TRENDS ---------------------------------------

pipeline_ps_trends <- list(
  tar_target(pop_change_counties,
             make_pop_change_counties(filepath = postcensal_pop_file)),
  tar_target(housing_change_counties,
             make_housing_change_counties(filepath = postcensal_housing_file)),
  tar_target(pop_hu_region_list,
             make_pop_hu_region_list(pop_change_counties, 
                                     housing_change_counties)),
  tar_target(pop_hu_change_cities,
             make_pop_hu_change_cities())
)

# MERGE PIPELINE PARTS ----------------------------------------------------

pipeline <- c(pipeline_files,
              pipeline_load_files,
              pipeline_load_into_pg,
              pipeline_create_pg_tables,
              pipeline_analysis,
              pipeline_ps_trends)


# RUN TARGET PIPELINE -----------------------------------------------------

pipeline
