# ABOUT ----
# This file defines functions used in the target plan (_targets.R)


# FUNCTIONS ---------------------------------------------------------------

load_landuse_codes <- function(filepath = ""){
 
  luc <- read_xlsx(filepath) |> 
    as_tibble() |> 
    clean_names()
  
  return(luc)
}

write_to_db <- function(x, table_name, overwrite = TRUE){
  
  db_host <- Sys.getenv("POSTGRES_HOST")
  db_port <- Sys.getenv("POSTGRES_PORT")
  db_name <- Sys.getenv("POSTGRES_NAME")
  db_user <- Sys.getenv("POSTGRES_USER")
  db_password <- Sys.getenv("POSTGRES_TOD_PASSWORD")
  
  
  con <- dbConnect(RPostgres::Postgres(),
                   dbname = db_name,
                   host = db_host,
                   port = db_port,
                   user = db_user,
                   password = db_password)
  
  on.exit(dbDisconnect(con))
  
  dbWriteTable(con, 
               table_name, 
               x, 
               overwrite = overwrite,
               row.names = FALSE)
  
  # Check is table exists
  
  q <- glue("
            SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = '{table_name}'
                  );
            ")
  
  table_exists <- dbGetQuery(con, q)
  
  return(table_exists)
  
}

write_sf_to_db <- function(x, table_name, overwrite = TRUE) {
  
  # Create database connection
  db_host <- Sys.getenv("POSTGRES_HOST")
  db_port <- Sys.getenv("POSTGRES_PORT")
  db_name <- Sys.getenv("POSTGRES_NAME")
  db_user <- Sys.getenv("POSTGRES_USER")
  db_password <- Sys.getenv("POSTGRES_TOD_PASSWORD")
  
  con <- dbConnect(RPostgres::Postgres(),
                   dbname = db_name,
                   host = db_host,
                   port = db_port,
                   user = db_user,
                   password = db_password)
  
  on.exit(dbDisconnect(con))
  
  # Create a DSN (Data Source Name) string for st_write
  dsn <- sprintf("PG:dbname='%s' host='%s' port='%s' user='%s' password='%s'",
                 db_name, db_host, db_port, db_user, db_password)
  
  if (overwrite){
    layer_options <- "OVERWRITE=true"
  } else {
    layer_options <- character(0)
  }
  
  
  # Use st_write to write the sf object to the database
  st_write(x, 
           dsn, 
           table_name, 
           append = !overwrite, 
           layer_options = layer_options)
  
  # Check if table exists
  q <- glue("
              SELECT EXISTS (
                      SELECT FROM information_schema.tables
                      WHERE table_schema = 'public' AND table_name = '{table_name}'
                    );
              ")
  table_exists <- dbGetQuery(con, q)
  
  return(table_exists)
}


load_zoning <- function(filepath = ""){
  
  z <- st_read(filepath) |> 
    as_tibble() |> 
    clean_names() |> 
    rename(geom = geometry)
  
  return(z)
  
}

