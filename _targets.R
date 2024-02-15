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
  packages = c("tibble","tidyverse","DBI","RPostgres","glue")
)

# SOURCE R FUNCTIONS ----

tar_source(files = here("R/functions.R"))

# DEFINE TARGET PLAN ----

list(
  tar_target(landuse_codes_file, 
             here("data/Washington State Standard Assessment 2-Digit Codes.xlsx"), 
             format = "file"),
  tar_target(landuse_codes, 
             load_landuse_codes(landuse_codes_file))
)
