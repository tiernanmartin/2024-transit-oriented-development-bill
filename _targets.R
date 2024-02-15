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

# SOURCE R FUNCTIONS ----

tar_source(files = here("R/functions.R"))

# PIPELINE PART: FILES ----

pipeline_files <- 
  list(
    tar_target(landuse_codes_file, 
               here("data/Washington State Standard Assessment 2-Digit Codes.xlsx"), 
               format = "file")
    
  )


# PIPELINE PART: POSTGRES ------------------------------------------------------


pipeline_postgres <- 
  list(
    tar_target(landuse_codes, 
               load_landuse_codes(landuse_codes_file))
  )


# MERGE PIPELINE PARTS ----------------------------------------------------


pipeline <- c(pipeline_files,
              pipeline_postgres)


# RUN TARGET PIPELINE -----------------------------------------------------

pipeline
