# library(DBI)

# # Function to establish connection to PostgreSQL database
# connect_to_db <- function() {
#   con <- dbConnect(RPostgres::Postgres(),
#                    dbname = "your_database_name",
#                    host = "host_name",
#                    port = 5432,
#                    user = "your_username",
#                    password = "your_password")
#   return(con)
# }
# 
# # Function to disconnect safely
# disconnect_db <- function(con) {
#   dbDisconnect(con)
# }
