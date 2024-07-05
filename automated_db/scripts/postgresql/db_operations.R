# library(DBI)
# 
# # Function to insert data into a table
# insert_data <- function(con, data, table_name) {
#   dbWriteTable(con, table_name, data, append = TRUE, row.names = FALSE)
# }
# 
# # Function to update data
# update_data_in_db <- function(con, query) {
#   dbExecute(con, query)
# }
# 
# # Function to fetch data
# fetch_data <- function(con, query) {
#   result <- dbGetQuery(con, query)
#   return(result)
# }
