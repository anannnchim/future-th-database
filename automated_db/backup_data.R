# Create a backup 

library(googlesheets4)
# This will prompt for Google account access
gs4_auth()

# Access the specific Google Sheet by name or URL
sheet <- sheets_get("https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit?gid=0#gid=0")
#sheet <- googlesheets4::gs4_get("https://docs.google.com/spreadsheets/d/19Rj7iW5xWOe6ZJJRsO9VzsZXyLfFu1S_vtClEE_3DEw/edit?gid=0#gid=0")
# Read data from the first sheet
new_data <- read_sheet(sheet, range = "Sheet1")
# Optionally filter data based on date or other criteria
library(dplyr)
last_date <- as.Date('last_date_from_your_database')  # Replace with a query to get the last date from your DB
new_data <- new_data %>% filter(date > last_date)






library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="system_f1_th", host="localhost", user="postgres", password="postgres")


dbWriteTable(con, "trading_data", new_data, append=TRUE, row.names=FALSE)
