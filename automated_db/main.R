# Main: This script automate and update the process.


# one way to do it.
lib <- modules::use("scripts")
result <- lib$math_basic$add_two_number(1,2)


# Required libraries
library(googlesheets4) # for Google Sheets
library(RPostgreSQL)   # for PostgreSQL
library(DBI)           # for general DB interaction
library(aws.rds)       # if using AWS RDS via an R package

# Helper functions
source("scripts/db_connect.R")
source("scripts/data_acquisition/scrape_raw_data.R")
source("scripts/data_cleaning/clean_and_reformat.R")
source("scripts/data_update/update_db.R")
source("scripts/data_update/update_rds.R")

main <- function() {
  # Set up connections
  try({
    
    # Connection 
    googlesheet_con <- connect_googlesheet()
    db_con <- connect_database()
    rds_con <- connect_rds() 
    
    # Import configuration data and input data
    configure_data <- get_configure_data(googlesheet_con)
    input_data <- import_input(googlesheet_con)
    
    # Check if the database needs updating-
    if (!is_db_updated(db_con)) {
      # Data acquisition and cleaning
      raw_data <- scrape_raw_data(input_data)
      clean_data <- clean_and_reformat_data(raw_data)
      
      # Update databases
      updated_db <- update_db(db_con, clean_data)  # Update PostgreSQL
      updated_rds <- update_rds(rds_con, clean_data) # Update RDS
      
      # Optionally, log updates
      log_update_status(updated_db, updated_rds)
    } else {
      message("Databases are already up-to-date. No action was taken.")
    }
  }, error = function(e) {
    cat("Error in main function: ", e$message, "\n")
  })
  
  # Close connections
  dbDisconnect(googlesheet_con)
  dbDisconnect(db_con)
  dbDisconnect(rds_con)
}

# Schedule this script to run daily

