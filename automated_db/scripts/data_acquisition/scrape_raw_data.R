# scrape raw 
library(tidyverse)
library(RSelenium)
library(dplyr)
library(rvest)
library(netstat)
library(data.table)




binman::list_versions("chromedriver")

rD <- rsDriver(browser = "chrome",
               chromever = "114.0.5735.90",port = 9515L, verbose = FALSE)


remDr <- client










# 
# 
# # Define the scraping function
# scrape_from_tfex <- function(symbol) {
#   # Constants
#   url <- paste0('https://www.tfex.co.th/en/products/currency/eur-usd-futures/', symbol, '/historical-trading')
#   xpath <- '//*[@id="__layout"]/div/div[2]/div[2]/div[2]/div/div[3]'
#   
#   # Start RSelenium
#   rD <- rsDriver(browser = "chrome", chromever = "latest")
#   remDr <- rD$client
#   
#   # Setup options
#   remDr$open()
#   remDr$navigate(url)
#   
#   # Wait for the table to load
#   table_element <- remDr$findElement(using = "xpath", value = xpath)
#   remDr$executeScript("arguments[0].scrollIntoView(true);", list(table_element))
#   Sys.sleep(5)  # Adjust sleep time if necessary
#   
#   # Get table HTML and parse
#   table_html <- table_element$getElementAttribute("outerHTML")[[1]]
#   table_data <- read_html(table_html) %>% html_table()
#   
#   # Define DataFrame
#   df <- as.data.frame(table_data[[1]])
#   names(df) <- c('Date', 'Open', 'High', 'Low', 'Close', 'SP', 'Chg', '%Chg', 'Vol (Contract)', 'OI (Contract)')
#   
#   # Add the 'Symbol' column
#   df$Symbol <- symbol
#   
#   # Stop RSelenium
#   remDr$close()
#   rD$server$stop()
#   
#   return(df)
# }
# 
# # Example usage
# result_df <- scrape_from_tfex("S50U24")
