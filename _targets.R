# LOAD PACKAGES ----

library(targets)

# SET TARGET OPTIONS ----
tar_option_set(
  packages = c("tibble","tidyverse","DBI","RPostgres")
)

# SOURCE R FUNCTIONS ----

tar_source(files = "R/functions.R") 

# GET DATABASE CONNECTION PARAMETERS

db_host <- Sys.getenv("DB_HOST")
db_port <- Sys.getenv("DB_PORT")
db_name <- Sys.getenv("DB_NAME")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("POSTGRES_TOD_PASSWORD")

# DEFINE TARGET PLAN ----

list(
  
)
