# LOAD PACKAGES ----

library(targets)

# SET TARGET OPTIONS ----
tar_option_set(
  packages = c("tibble")
)

# SOURCE R FUNCTIONS ----

tar_source(files = "R/functions.R") 

# DEFINE TARGET PLAN ----

list(
  
)
