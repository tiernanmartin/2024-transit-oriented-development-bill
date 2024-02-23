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
      category_short %in% "recreation" & str_detect(use, "Parks") ~ TRUE,
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

make_pop_change_counties <- function(filepath = ""){
  
  pop_df <- read_excel(filepath, sheet = "Population", skip = 3) |> 
    clean_names()
  
  ps_counties <- c("King",
                   "Snohomish",
                   "Kitsap",
                   "Pierce")
  
  pop_change_counties <- pop_df |> 
    filter(filter %in% "1") |>  
    pivot_longer(cols = x_1960_census_count_of_total_population:last_col()) |> 
    mutate(value = as.numeric(value),
           pugetsound = case_when(
             county %in%  ps_counties ~ TRUE,
             TRUE ~ FALSE
           )) |> 
    filter(str_detect(name, "2010|2023")) |> 
    filter(str_detect(name,"postcensal")) |> 
    mutate(name = str_c("pop_", str_extract(name,"\\d{4}"))) |> 
    pivot_wider(names_from = name,
                values_from = value) |> 
    group_by(pugetsound) |> 
    summarise(pop_2010 = max(pop_2010),
              pop_2023 = max(pop_2023),
              pop_change = sum(pop_2023) - sum(pop_2010)) |> 
    ungroup() |> 
    mutate(pop_change_pct = round(digits = 2, pop_change / pop_2010))
  
  return(pop_change_counties)
}

make_housing_change_counties <- function(filepath = ""){
  
  hu_df <- read_excel(filepath, sheet = "Housing Units", skip = 3) |>  
    clean_names()
  
  housing_change_counties <- hu_df |> 
    filter(filter %in% "1") |> 
    pivot_longer(cols = x1980_census_count_of_total_housing_units:last_col()) |> 
    mutate(value = as.numeric(value),
           pugetsound = case_when(
             county %in% c("King",
                           "Snohomish",
                           "Kitsap",
                           "Pierce") ~ TRUE,
             TRUE ~ FALSE
           )) |> 
    filter(str_detect(name,"total")) |> 
    filter(str_detect(name, "2010|2023")) |>  
    mutate(name = str_c("hu_", str_extract(name,"\\d{4}"))) |> 
    pivot_wider(names_from = name,
                values_from = value) |> 
    group_by(pugetsound) |> 
    summarise(.groups = "drop",
              hu_2010 = sum(hu_2010, na.rm = TRUE),
              hu_2023 = sum(hu_2023, na.rm = TRUE),
              hu_change = sum(hu_2023,na.rm = TRUE) - sum(hu_2010, na.rm = TRUE)) |> 
    mutate(hu_change_pct = round(digits = 2, hu_change / hu_2010))
    
  
  return(housing_change_counties)
}

make_pop_hu_region_list <- function(pop_change_counties,
                                    housing_change_counties){
  lst <- pop_change_counties |> 
    select(-pugetsound) |> 
    bind_cols(housing_change_counties) |>  
    mutate_at(vars(matches("pct")),scales::percent) |> 
    mutate_at(vars(pop_2010:pop_change),scales::comma) |> 
    mutate_at(vars(hu_2010:hu_change),scales::comma) |>
    pivot_longer(cols = c(starts_with("pop"),starts_with("hu"))) |> 
    transmute(region = if_else(pugetsound,"pugetsound","outside"),name,value) |> 
    transmute(name = str_c(region,name,sep = "_"),value) |> 
    tbl_to_named_list("value","name")
  
  return(lst)
}

make_pop_hu_change_cities <- function(){
  
  df_cities <- get_acs(
    geometry = TRUE, 
    geography = "place", 
    variables = c(population = "B01003_001",
                  housing_units = "B25001_001"), 
    state = "WA", 
    year = 2022, 
    survey = "acs5" 
  ) |> clean_names()
  
  cities_sf <- df_cities |> 
    filter(variable == "population") |> 
    select(geoid)
  
  pop_hu_change_cities <- df_cities |> 
    st_drop_geometry() |> 
    select(geoid, name, variable, estimate) |> 
    filter(str_detect(name,"city")) |> 
    mutate(name = str_remove(name," city, Washington")) |> 
    pivot_wider(names_from = variable, values_from = estimate) |> 
    left_join(cities_sf, by = join_by(geoid)) |> 
    st_sf()
  
  return(pop_hu_change_cities)
}

make_analysis_transit_walksheds <- function(target_dependencies = list()){
  
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
  
  analysis_transit_walksheds <- st_read(con, "transit_walksheds") |> 
    as_tibble() |> 
    st_sf()
  
  return(analysis_transit_walksheds)
  
}

make_analysis_study_groups <- function(analysis_parcels_ndc){
  
  analysis_study_total <- analysis_parcels_ndc |> 
    st_drop_geometry() |> 
    mutate(dev_capacity_added = poss_multiply(analysis_ndc_uniform, area_ft2)
    ) |> 
    summarize(n = n(),
              area_mi2_smry = sum(area_mi2, na.rm = TRUE),
              dev_capacity_added_smry = sum(dev_capacity_added, na.rm = TRUE),
              awmndc_smry = round(digits = 2,
                                  weighted.mean(na.rm = TRUE, 
                                                analysis_ndc_uniform, 
                                                w = area_mi2))
    ) |> 
    transmute(
      name = "analysis_total",
      value = "Total",
      n_tbl = comma(n),
      area_tbl = comma(area_mi2_smry),
      devcap_tbl = format_nbr(dev_capacity_added_smry),
      awmndc_tbl = as.character(awmndc_smry))
  
  analysis_study_groups <- analysis_parcels_ndc |> 
    st_drop_geometry() |> 
    mutate(analysis_type_uni = factor(analysis_type_uni, 
                                      levels = c("Not Developable", 
                                                 "Developable, Not Affected",
                                                 "Developable, Affected")
    ),
    dev_capacity_added = poss_multiply(analysis_ndc_uniform, area_ft2)
    ) |> 
    pivot_longer(cols = c(analysis_type_uni, analysis_station_area_types)) |> 
    group_by(name, value) |> 
    summarize(n = n(),
              area_mi2_smry = sum(area_mi2, na.rm = TRUE),
              dev_capacity_added_smry = sum(dev_capacity_added, na.rm = TRUE),
              awmndc_smry = round(digits = 2,
                                  weighted.mean(na.rm = TRUE, 
                                                analysis_ndc_uniform, 
                                                w = area_mi2))
    ) |> 
    mutate(n_pct = n / sum(n),
           area_pct = area_mi2_smry / sum(area_mi2_smry),
           dev_cap_pct = dev_capacity_added_smry / sum(dev_capacity_added_smry, na.rm = TRUE)) |> 
    ungroup() |> 
    transmute(
      name,
      value,
      n_tbl = glue("{comma(n)} ({percent(n_pct)})"),
      area_tbl = glue("{comma(area_mi2_smry)} ({percent(area_pct, accuracy = 1)})"),
      devcap_tbl = if_else(dev_capacity_added_smry<=0,
                           "--",
                           glue("{format_nbr(dev_capacity_added_smry)} ({percent(dev_cap_pct)})")),
      awmndc_tbl = if_else(is.nan(awmndc_smry),
                           '--',
                           as.character(awmndc_smry))
    )
  
  analysis_study_groups_ready <- bind_rows(analysis_study_total,
                                           analysis_study_groups)
  
  return(analysis_study_groups_ready)
}


# UTILITY FUNCTIONS -----

gt_custom_summary <- function(x, df, start_col=3, end_col=6) { 
  for(col in start_col:end_col) { 
    
    if(identical(x, df[-1, col][[1]])) {
      
      return(df[1, col][[1]])
    }
  } 
  
  return(NULL)
}

format_nbr <- \(x) label_number(scale_cut = cut_short_scale(), accuracy = 0.01)(x)

poss_multiply <- possibly(function(x,y) x*y, otherwise = NA,quiet = TRUE)

weighted_mean <- possibly(.f = function(x,w){weighted.mean(x, w, na.rm = TRUE)},
                          otherwise = NA)

awmndc_fct <- function(ndc, wt, excl, ndc_max = 5){ 
  
  breaks <- c(-Inf, seq(0, ndc_max, by = 1))
  
  levels <- c( 
    "Not Developable",
    "Developable, Not Affected", 
    # "Less than 0", # remove -- same as 'Developable, Not Affected'
    paste(seq(0, ndc_max - 1, by = 1), "-", seq(1, ndc_max, by = 1), sep = "")
    ) |> rev()
  
  breaks <- c(-Inf, 0:ndc_max)
  
  if(all(!is.na(excl))){return(fct("Not Developable", levels = levels))}
  
  if(all(is.na(ndc))){return(fct("Developable, Not Affected", levels = levels))}
  
  awmndc <- weighted.mean(na.rm = TRUE, x = ndc, w = wt)
  
  awmnd_ordinal <- cut(awmndc, breaks = breaks, labels = levels[length(levels)-1:length(levels)])
  
  awmnd_factor <- fct(as.character(awmnd_ordinal), levels = levels)
  
  return(awmnd_factor)
  
}

tbl_to_named_list <- function(df,values,names){
  split(pluck(df,values), pluck(df,names))
}

pct_to_dec <- function(x) {
  # Remove the '%' sign and convert to numeric
  numeric_values <- as.numeric(sub("%", "", x))
  
  # Convert to decimal
  decimal_values <- numeric_values / 100
  
  return(decimal_values)
}

convert_to_mi2 <- function(x){x/27878400}

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


