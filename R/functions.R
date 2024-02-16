# ABOUT ----
# This file defines functions used in the target plan (_targets.R)



# TARGET FUNCTIONS ----

load_transit_hct <- function(cr, lr, lr_alt, sc, brt, proj_crs = 2926L){
  
  # Load data
  
  lr <- st_read(lr) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "LR",
              stop_name) |> 
    rename(geom = geometry)
  
  lr_alt<- st_read(lr_alt) |>  # this is used to clean up the other lightrail data set
    st_transform(proj_crs) |> 
    clean_names() |> 
    transmute(stop_name_other = name)
  
  sc <- st_read(sc) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "SC",stop_name) |> 
    rename(geom = geometry)
  
  cr <- st_read(cr) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "CR",stop_name) |> 
    rename(geom = geometry)
  
  brt <- st_read(brt) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "BRT",stop_name) |> 
    rename(geom = geometry)
  
 # Clean data
  
  lr <- lr |> distinct(geom, .keep_all = TRUE)
  
  lr_ready <- st_join(lr, 
                      lr_alt, 
                      join = st_is_within_distance, 
                      dist = 1500) |>
    mutate(stop_name = case_when(
      is.na(stop_name) ~ stop_name_other,
      TRUE ~ stop_name
    )) |> 
    select(-stop_name_other) |> 
    distinct() |> 
    select(mode, stop_name, geom)
  
  
  # Combine data
  
  transit_hct <- rbind(lr_ready,sc,cr,brt)
  
  
  return(transit_hct)
}

load_zoning_details <- function(filepath = ""){
  
  zd <- read_csv(filepath) |> 
    as_tibble() |> 
    clean_names()
  
  return(zd)
}

load_uga <- function(filepath = ""){
  
  uga <- st_read(filepath) |> 
    as_tibble() |> 
    clean_names() |> 
    rename(geom = geometry)
  
  return(uga)
  
}

load_zoning <- function(filepath = ""){
  
  z <- st_read(filepath) |> 
    as_tibble() |> 
    clean_names() |> 
    rename(geom = geometry)
  
  return(z)
  
}

load_landuse_codes <- function(filepath = ""){
 
  luc <- read_xlsx(filepath) |> 
    as_tibble() |> 
    clean_names()
  
  return(luc)
}

# UTILITY FUNCTIONS -----

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

write_to_db_ogr2ogr <- function(filepath, table_name){
  
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
  
  # Create the command to pass to system()
  
  command <- glue('ogr2ogr -f "PostgreSQL" PG:"dbname={Sys.getenv("POSTGRES_NAME")} user={Sys.getenv("POSTGRES_USER")} password={Sys.getenv("POSTGRES_TOD_PASSWORD")}" {filepath} -nln {table_name} -overwrite -progress')
  
  # Run the command
  system(command)
  
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


