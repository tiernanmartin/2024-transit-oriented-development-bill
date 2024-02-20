# ABOUT ----
# This file defines functions used in the target plan (_targets.R)



# TARGET FUNCTIONS ----

load_transit_hct <- function(cr, lr, lr_alt, sc, brt, proj_crs = 2926L){
  
  # Load data
  
  lr <- st_read(lr) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "LR",
              stop_name) |> 
    rename(geom = geometry) |> 
    st_sf()
  
  lr_alt<- st_read(lr_alt) |>  # this is used to clean up the other lightrail data set
    st_transform(proj_crs) |> 
    clean_names() |> 
    transmute(stop_name_other = name) |> 
    st_sf()
  
  sc <- st_read(sc) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "SC",stop_name) |> 
    rename(geom = geometry) |> 
    st_sf()
  
  cr <- st_read(cr) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "CR",stop_name) |> 
    rename(geom = geometry) |> 
    st_sf()
  
  brt <- st_read(brt) |> 
    st_transform(proj_crs) |> 
    transmute(mode = "BRT",stop_name) |> 
    rename(geom = geometry) |> 
    st_sf()
  
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

load_cities <- function(filepath = "", proj_crs = 2926L){
  
  cities <- st_read(filepath) |>
    st_transform(proj_crs) |> 
    as_tibble() |> 
    clean_names() |> 
    rename(geom = geometry) |> 
    st_sf()
  
  return(cities)
  
}

load_uga <- function(filepath = "", proj_crs = 2926L){
  
  uga <- st_read(filepath) |> 
    st_transform(proj_crs) |> 
    as_tibble() |> 
    clean_names() |> 
    rename(geom = geometry) |> 
    st_sf()
  
  return(uga)
  
}

load_zoning <- function(filepath = "", proj_crs = 2926L){
  
  z <- st_read(filepath) |> 
    st_transform(proj_crs) |> 
    as_tibble() |> 
    clean_names() |> 
    rename(geom = geometry) |> 
    st_sf()
  
  return(z)
  
}

load_landuse_codes <- function(filepath = ""){
 
  luc <- read_xlsx(filepath) |> 
    as_tibble() |> 
    clean_names()
  
  return(luc)
}

make_parcels_revised <- function(target_dependencies = list()){
  
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
  
  p <- st_read(con, layer = "parcels_walkshed_zoning")
  
  parcels_revised <- p |> 
    mutate(
      zoning_allow_resi = case_when(zoning_zone_link %in% "Seattle-IDM" ~ "Y",
                                    zoning_zone_link %in% "Seattle-IDR" ~ "Y",
                                    TRUE ~ zoning_allow_resi),
      zoning_height_ft = case_when(zoning_zone_link %in% c("Seattle-IDM") ~ 85, 
                                   zoning_zone_link %in% c("Seattle-IDR") ~ 170, 
                                   TRUE ~ zoning_height_ft),
      zoning_max_lot_coverage = case_when(zoning_zone_link %in% c("Seattle-IDM") ~ "100%", 
                                          zoning_zone_link %in% c("Seattle-IDR") ~ "65%", 
                                          TRUE ~ zoning_max_lot_coverage)
    )
  
  return(parcels_revised)
  
}

make_excluded_landuse_categories <- function(target_dependencies = list()){
  
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
  
  lu <- dbReadTable(con,"landuse_codes") |> 
    tibble()
  
  excluded_landuse_categories <- 
    lu |> 
    mutate(excluded = case_when(
      category_short %in% "manufacturing" ~ TRUE,
      category_short %in% "transport/utils" & !str_detect(use,"parking") ~ TRUE,
      category_short %in% "resources" ~ TRUE,
      category_short %in% "services" & str_detect(use,"Gov|Edu") ~ TRUE,
      category_short %in% "undeveloped" & code %in% c(95, 94, 93, 92) ~ TRUE,
      TRUE ~ FALSE
    ))
  
  return(excluded_landuse_categories)
  
}

circularity <- function(df){
  
  # This method has issues (e.g., condos)
  # Save it but don't implement
  
  circularity <- function(area, perimeter){
    (4 * pi * area) / (perimeter^3)
  }
  
  identify_outliers <- function(x) { 
    
    Q1 <- quantile(x, 0.25)
    Q3 <- quantile(x, 0.75)
    
    IQR <- Q3 - Q1
    
    # Define the lower and upper bounds for outliers
    lower_bound <- Q1 - 1.5 * IQR
    upper_bound <- Q3 + 1.5 * IQR
    
    # Is outlier
    is_outlier <- x[x < lower_bound | x > upper_bound]
    
    return(outliers)
  }
  
  
  df |> 
  transmute(city = zoning_juris,
            c = circularity(parcel_area, parcel_length)) |>
  mutate(q1 = quantile(c, 0.25,na.rm = TRUE),
         q3 = quantile(c, 0.75,na.rm = TRUE),
         iqr = q3 - q1,
         lower = q1 - (1.5*iqr),
         upper = q3 + (1.5*iqr),
         is_outlier = case_when(
           c < lower ~ TRUE,
           c > upper ~ TRUE,
           TRUE ~ FALSE
         ),
         is_outlier_alt = case_when(
           c < 0.01 ~ FALSE, # threshold that is obvious from the histogram
           TRUE ~ TRUE
         )) 
  }

make_parcels_ndc <- function(p = analysis_parcels_revised,
                             excl_lu = analysis_excluded_landuse_categories){
  
  parcel_excl_lu <- p |> 
    left_join(excl_lu, by = join_by(parcel_lu_code_category == category_short,
                                    parcel_lu_code_use == use))
  
  parcels_ndc <- parcel_excl_lu |> 
    rowwise() |>
    transmute(
      county = parcel_address_county,
      city = zoning_juris,
      transit_within_lr_walkshed,
      transit_within_cr_walkshed,
      transit_within_sc_walkshed,
      transit_within_brt_walkshed,
      transit_walkshed_desc,
      land_use = parcel_lu_code_category,
      area_ft2 = parcel_area,
      area_mi2 = convert_to_mi2(area_ft2),
      district_name = zoning_district_name,
      max_bldg_ftprt = zoning_max_bldg_ftprt,
      max_far = zoning_max_far,
      max_lot_coverage = pct_to_dec(zoning_max_lot_coverage),
      height_stories = zoning_height_stories,
      height_ft = zoning_height_ft,
      excl_lu_code = excluded,
      excl_resi_not_allowed = case_when(
        zoning_allow_resi %in% "N" ~ TRUE, # invert from 'allow' to 'not allow'
        TRUE ~ FALSE
      ),
      excl_reason = case_when(
        excl_lu_code & excl_resi_not_allowed ~ glue("Land Use: {land_use}; Resid.: Not Allowed"),
        excl_lu_code ~ glue("Land Use: {land_use}"),
        excl_resi_not_allowed ~ glue("Resid.: Not Allowed"),
        TRUE ~ NA_character_
      ),
      est_max_bldg_ftprt_pct = case_when(
        (max_bldg_ftprt / area_ft2) > 1 ~ 1, # put a ceiling on this percentage
        TRUE ~ round(digits = 2, max_bldg_ftprt / area_ft2)),
      est_max_lot_pct = case_when(
        !is.na(max_lot_coverage) & !is.na(est_max_bldg_ftprt_pct) & max_lot_coverage < est_max_bldg_ftprt_pct ~ max_lot_coverage,
        !is.na(max_lot_coverage) & !is.na(est_max_bldg_ftprt_pct) & max_lot_coverage > est_max_bldg_ftprt_pct ~ est_max_bldg_ftprt_pct,
        !is.na(max_lot_coverage) ~ max_lot_coverage,
        !is.na(est_max_bldg_ftprt_pct) ~ est_max_bldg_ftprt_pct,
        TRUE ~ 1 # This assumes 100% lot coverage when no max footprint or coverage
      ),
      est_max_stories = case_when(
        ! is.na(height_stories) ~ height_stories,
        ! is.na(height_ft) ~ est_height_stories(height_ft),
        TRUE ~ NA_integer_),
      est_max_far = case_when(
        excl_lu_code ~ NA_real_, # FAR is NA if p has excluded land use
        excl_resi_not_allowed ~ NA_real_, # FAR is NA if p doesnt allow resi
        ! is.na(max_far) ~ max_far,
        TRUE ~  round(digits = 2, est_max_lot_pct * est_max_stories)
      ),
      est_max_far_type = case_when(
        is.na(est_max_far) ~ NA,
        ! is.na(max_far) ~ "actual",
        TRUE ~ "estimate" 
      ),
      est_max_far_hb2160 = 
        case_when(
          transit_within_lr_walkshed ~ 3.5, 
          transit_within_sc_walkshed ~ 3.5,
          transit_within_cr_walkshed ~ 3.5,
          transit_within_brt_walkshed ~ 2.5,
          TRUE ~ NA_real_
        ),
      analysis_station_area_types = case_when(
        transit_within_lr_walkshed ~ "Large (0.5 Mile)",
        transit_within_cr_walkshed ~ "Large (0.5 Mile)",
        transit_within_sc_walkshed ~ "Large (0.5 Mile)",
        transit_within_brt_walkshed ~ "Small (0.25 Mile)",
        TRUE ~ NA),
      analysis_ndc = est_max_far_hb2160 - est_max_far,
      analysis_ndc_uniform = case_when(
        is.na(analysis_ndc) ~ NA_real_,
        analysis_ndc < 0 ~ NA_real_,
        TRUE ~ analysis_ndc # only calculate NDC for parcels below HB 2160's FAR thresholds
      ),
      analysis_type_uni = case_when(
        is.na(analysis_ndc) ~ "Not Developable",
        analysis_ndc < 0 ~ "Developable, Not Affected",
        TRUE ~ "Developable, Affected"
      ),
      analysis_type_avg = case_when(
        is.na(analysis_ndc) ~ "Not Developable",
        TRUE ~ "Developable"
      )
    ) |> 
    ungroup()
  
  return(parcels_ndc)
}

# UTILITY FUNCTIONS -----

pct_to_dec <- function(x) {
  # Remove the '%' sign and convert to numeric
  numeric_values <- as.numeric(sub("%", "", x))
  
  # Convert to decimal
  decimal_values <- numeric_values / 100
  
  return(decimal_values)
}

convert_to_mi2 <- function(x){round(digits = 2, x/27878400)}

est_height_stories <- function(x){
  
  if(is.na(x)){
    return(NA)
  }
  
  if(x<=30){
    estimate <- floor(x/10)
    return(estimate)
  } 
  
  if(x>3){
    base_floor <- 15
    upper_floors <- floor((x - base_floor) / 10)
    estimate <- 1 + upper_floors
    return(estimate)
  }
  
}

test_psql <- function(){
  system(glue("psql -U {Sys.getenv('POSTGRES_USER')} --version"))
}

run_sql_query <- function(filepath = "", table_name = "", target_dependencies = list()){ 
  
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
  
  
  # Create psql command
  
  psql_command <- glue("psql -h {db_host} -d {db_name} -U {db_user} -p {db_port} -a -f {filepath}")
  
  # IMPORTANT: 
  # This psql command requires PostgreSQL authentication to access the database.
  # Ensure that your PostgreSQL password is securely stored in the pgpass.conf file.
  # This file should be located at %APPDATA%\postgresql\pgpass.conf on Windows systems.
  # The pgpass.conf file must follow the format: hostname:port:database:username:password
  
  
  # Execute the command
  
  system(psql_command)
  
  # Check if table exists
  
  check_query <- glue("
            SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = '{table_name}'
                  );
            ")
  
  table_exists <- dbGetQuery(con, check_query)
  
  return(table_exists)
  
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


