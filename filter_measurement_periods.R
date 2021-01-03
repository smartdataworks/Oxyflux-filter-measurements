#!/usr/bin/Rscript
# !diagnostics off

# Author: Emanuel Blei
# Institution: University of Goettingen, Germany
# Year: 2020

# This script identifies the latest background measurement periods and then downloads
# the relevant data and calculate the average background concentration.
# It then downloads the chamber measurement data and calculates the differential
# concentration values for each chamber enclosure period.
##################################################################################################
##################################################################################################
##################################### Load packages ##############################################
##################################################################################################
##################################################################################################
library(methods)
library(tidyverse)
library(magrittr)
library(data.table)
library(hablar)
library(lubridate)


##################################################################################################
##################################################################################################
########################################### Settings #############################################
##################################################################################################
##################################################################################################
Sys.setenv(TZ='UCT')

##################################################################################################
##################################################################################################
############################ Define some initial helper functions ################################
##################################################################################################
##################################################################################################
source("filter_measurement_scripts/general_helper_functions.R")


##################################################################################################
##################################################################################################
################################## InfluxDB interaction functions ################################
##################################################################################################
##################################################################################################
# These functions use the previously defined query functions to generate query texts that are then
# sent to InfluxDB before being processed.

source("filter_measurement_scripts/query_processing_functions.R")


##################################################################################################
##################################################################################################
######################### Functions that transform data tables and lists #########################
##################################################################################################
##################################################################################################

source("filter_measurement_scripts/data_structure_transformers.R")

##################################################################################################
##################################################################################################
######################################### Load background data ###################################
##################################################################################################
##################################################################################################

# We start with the buffer and branch chamber data.
# We have to calculate the concentrations during each calibration period.
# We do this one period at a time. 
# We also have to then make a linear model for the time in between calibrations.
# We have to do this two periods at a time. 
# We also have to account for the time delays between the actual activation 
# and the signal coming out of the Licor 820/840 and Calvin:
# The chamber/buffer/cylinder to base delay is 50 seconds. 
# The base to Calvin delay is 230 seconds.
# Adding this up the chamber/buffer/cylinder to Calvin delay is 280 seconds.

background.columns <- setNames(c("Buffer", "CylAir"),
                               c("buffer", "cylinder"))


last.measurement.times.and.periods <- get_new_start_times_and_period_nos()
new.start.time <- max(last.measurement.times.and.periods$time)

# Run all the previous functions to load and process the newest measurements:
new.backgrounds <- lapply(background.columns, 
                          get_new_background_periods, 
                          start.time=new.start.time) %>% 
  remove_empty_list_elements()


background.data <- lapply(new.backgrounds, function(dt) 
  dt[, call_background_data(.SD), by=period.no]) %>% 
  lapply(remove_failed_backgrounds)


# This prevents the script running any further if there is no new background data
all.background.data.is.empty <- all(vapply(background.data, 
                                           function(dt){dt[, .N] < 2 || is.null(dt)}, 
                                           FUN.VALUE = logical(1)))

if(all.background.data.is.empty){
  print("There is no background data to process.")
} else {
  
  ##################################################################################################
  ##################################################################################################
  ####################################### Load chamber data ########################################
  ##################################################################################################
  ##################################################################################################
  # Finally apply the second lot of functions to load and calculate all the chamber values
  # for all three chamber types. I also put a test in so that the script does not die if 
  # there is no background data of either kind. 
  
  buffer.data.is.empty <-  is.null(background.data$buffer) || background.data$buffer[, .N] < 2L
  cylinder.data.is.empty <- is.null(background.data$cylinder) || background.data$cylinder[, .N] < 2L
  
  chamber.data <- list()
  if(!buffer.data.is.empty){
    chamber.data$branch <- roll_function(background.data$buffer, get_branch_measurements)
  }
  if(!cylinder.data.is.empty){
    chamber.data$soil <- roll_function(background.data$cylinder, get_soil_measurements)
    chamber.data$stem <- roll_function(background.data$cylinder, get_stem_measurements)
  }
  
  all.chamber.data.is.empty <- check_if_nested_df_list_is_empty(chamber.data)
  
  if(all.chamber.data.is.empty){
    print("There is no chamber data to process.")
  } else {
    
    chamber.data %<>% lapply(remove_empty_list_elements)
    chamber.data %<>% remove_empty_list_elements()
    
    # Combine the chamber data and update period numbers.
    chamber.data %<>% lapply(function(x) do.call(rbind, x)) %>% 
      lapply(segregate_into_periods) %>% 
      add_previous_period_nos_to_chamber_data(last.measurement.times.and.periods) %>% 
      lapply(ensure_type_safety)
    
    ##################################################################################################
    ##################################################################################################
    ############################### Write data back to InfluxDB ######################################
    ##################################################################################################
    ##################################################################################################
    # Make a named vector of InfluxDB measurement names.
    measurement.names <- paste(names(chamber.data), "chamber_measurements", sep="_")
    
    # Run the functions that write everything to InfluxDB.
    mapply(write_chamber_data_to_influxdb,
           dt=chamber.data,
           measurement=measurement.names)
    
    print("Script ran successfully.")
  }
}
