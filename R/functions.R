# ABOUT ----
# This file defines functions used in the target plan (_targets.R)

load_landuse_codes <- function(filepath = ""){
 
  luc <- read_xlsx(filepath) |> 
    as_tibble() |> 
    clean_names()
  
  return(luc)
}

write_to_db <- function(x, table_name, overwrite = FALSE){
  
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
               append = FALSE,
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
