#!/usr/bin/Rscript
# !diagnostics off


##################################################################################################
##################################################################################################
#################################### Load helper functions #######################################
##################################################################################################
##################################################################################################
source("filter_measurement_scripts/general_helper_functions.R")


##################################################################################################
##################################################################################################
###################################### Background queries ########################################
##################################################################################################
##################################################################################################
# These functions create query texts that can then be sent off to InfluxDB. The actual query calls 
# are made in separate functions.

get_switch_column_names <- function(){
  setNames(c(paste0("AK", 1:4), 
             "Buffer", 
             paste0("BK", 1:8), 
             "CylAir", 
             paste0("SK", 1:4)), 
           c(paste("branch.chamber", 1:4, sep="."),
             "buffer",
             paste("soil.chamber", 1:8, sep="."),
             "cylinder",
             paste("stem.chamber", 1:4, sep=".")))
}

avoid_overlap_first_condition <- function(){
  
  switch.columns <- get_switch_column_names()
  
  paste(paste0(switch.columns, collapse=" + "), "= 1")
}


# avoid_overlap_second_condition <- function(id){
#   
#   if(id %in% c(paste0("AK", 1:4), "Buffer")){
#     return("AND TopSwitch = 1 ")
#   } else {
#     return("AND TopSwitch = 0 ")
#   } 
# }

avoid_overlap_second_condition <- function(){
  paste0("((", paste0(c(paste0("AK", 1:4), "Buffer"), collapse=" + "), " = 1 AND ",
         "TopSwitch = 1) ",
         "OR TopSwitch = 0)")
}


# avoid_overlap <- function(id){
#   # This function simply returns a string that can be added to a query to 
#   # avoid periods where more than one chamber/buffer/cylinder is active.
#   first_condition <- avoid_overlap_first_condition()
#   second_condition <- avoid_overlap_second_condition(id)
#   
#   return(paste(first_condition, second_condition))
# }


avoid_overlap <- function(){
  # This function simply returns a string that can be added to a query to 
  # avoid periods where more than one chamber/buffer/cylinder is active.
  first_condition <- avoid_overlap_first_condition()
  second_condition <- avoid_overlap_second_condition()
  
  return(paste0(first_condition, " AND ", second_condition))
}


# stopifnot(exprs={
#   identical(avoid_overlap("AK1"), paste("AND AK1 + AK2 + AK3 + AK4 +", 
#             "Buffer + BK1 + BK2 + BK3 + BK4 + BK5 + BK6 + BK7 +", 
#             "BK8 + CylAir + SK1 + SK2 + SK3 + SK4 <= 1 AND TopSwitch = 1 "))
#   identical(avoid_overlap("Buffer"), paste("AND AK1 + AK2 + AK3 + AK4 +", 
#                                         "Buffer + BK1 + BK2 + BK3 + BK4 + BK5 + BK6 + BK7 +", 
#                                         "BK8 + CylAir + SK1 + SK2 + SK3 + SK4 <= 1 AND TopSwitch = 1 "))
#   identical(avoid_overlap("CylAir"), paste("AND AK1 + AK2 + AK3 + AK4 +", 
#                                         "Buffer + BK1 + BK2 + BK3 + BK4 + BK5 + BK6 + BK7 +", 
#                                         "BK8 + CylAir + SK1 + SK2 + SK3 + SK4 <= 1 AND TopSwitch = 0 "))
# })


previous_measurement_period_query <- function(measurement){
  paste("SELECT period_no FROM", measurement, "ORDER BY time DESC LIMIT 1;")
}


last_measurement_query <- function(background.id){
  paste0("SELECT LAST(", background.id, ") AS id FROM raw_base;")
}


last_non_background_measurement_query <- function(background.id, time.stamp){
  paste0("SELECT LAST(", background.id, ") AS id FROM raw_base WHERE ", background.id, " != 1 ",
         "AND time <= \'", time.stamp, "\';")
}


active_measurement_query <- function(background.id, start.time=NULL, end.time=NULL){
  # Creates query text for InfluxDB.
  
  # Check if a start.time argument was supplied. If not use the default.
  start.time <- start.time %||% "2020-05-12 00:00:00"
  # Check if start.time argument is a character rather than POSIXt. If so convert to POSIXt.
  if(is.character(start.time)){
    start.time <- as.POSIXct(start.time,  tz="UTC")
  }
  start.time <- paste0("\'", start.time, "\'")
  
  # Check if end.time argument was supplied. Otherwise use current time as default.
  end.time <- end.time %||% now(tzone="UTC")
  # Check if end.time argument is a character rather than POSIXt. If so convert to POSIXt.
  if(is.character(end.time)){
    end.time <- as.POSIXct(end.time,  tz="UTC")
  }
  
  end.time <- paste0("\'", end.time, "\'")
  
  output.string <- paste0("SELECT ", background.id, " AS id ",
                          "FROM raw_base WHERE ", background.id, " = 1 ",
                          "AND ", avoid_overlap(), " ",
                          "AND time > ", start.time, " ",
                          "AND time < ", end.time, ";")
  
  return(output.string)
}


##################################################################################################
##################################################################################################
######################################### Chamber queries ########################################
##################################################################################################
##################################################################################################

make_measurement_period_query <- function(chamber.id, start.time, end.time){
  # Make a query string that will be sent to InfluxDB to get the start and end
  # times of active measurement periods for one chamber. 
  # This is intended for any type of chamber.
  
  paste0("SELECT ", chamber.id, " AS id FROM raw_base ",
         "WHERE ", chamber.id, " = 1 ",
         "AND time >= \'", start.time, "\' ",
         "AND time <= \'", end.time, "\';")
}


make_background_base_query <- function(start.time, end.time){
  paste0("SELECT CO2 AS base_CO2 FROM raw_base WHERE time >= \'", start.time,
         "\' AND time <= \'", end.time, "\';")
}


make_filter_query <- function(start.time, end.time){
  
  paste0("SELECT CO2 as dummy FROM raw_base ",
         "WHERE time >= \'", start.time,
         "\' AND time <= \'", end.time, "\' ",
         "AND ", avoid_overlap(), ";")
}


make_calvin_query <- function(start.time, end.time){
  paste0("SELECT CO2_average AS calvin_CO2, ",
         "O2_perMeg_value AS calvin_O2 ",
         "FROM raw_calvin WHERE time >= \'", start.time,
         "\' AND time <= \'", end.time, "\';")
}


make_chamber_base_query <- function(start.time, end.time, chamber.id){
  paste0("SELECT CO2 AS base_CO2, FlowSLM_", chamber.id, 
         " AS flow_rate FROM raw_base WHERE time >= \'", start.time,
         "\' AND time <= \'", end.time, "\';")
}


make_chamber_sensor_queries <- list(
  
  branch = function(chamber.id, start.time, end.time){
    # Make a query string that will be sent to InfluxDB to retrieve the 
    # meteorological measurement data for a branch chamber. 
    
    chamber.id %<>% gsub("K", "k", .) 
    start.time %<>% as.character
    end.time %<>% as.character
    
    paste0("SELECT PAR_", chamber.id, " AS PAR, ",
           "Temp_", chamber.id, " AS air_temperature, ",
           "Hum_", chamber.id, " AS relative_humidity, ",
           "LeafTemp_", chamber.id, " AS leaf_temperature, ",
           "Ground_press_hPa AS barometric_pressure ",
           "FROM raw_branch ",
           "WHERE time >= \'", start.time, "\' AND ",
           "time <= \'", end.time, "\';" )
  },
  
  soil = function(chamber.id, start.time, end.time){
    # Make a query string that will be sent to InfluxDB to retrieve the 
    # meteorological measurement data for a soil chamber. 
    
    chamber.id %<>% gsub("K", "k", .) 
    start.time %<>% as.character
    end.time %<>% as.character
    
    paste0("SELECT Temp_", chamber.id, " AS air_temperature, ",
           "PressTmp_", chamber.id, " AS pressure_sensor_temperature, ",
           "SoilTemp_", chamber.id, " AS soil_temperature, ",
           "Moisture_", chamber.id, " AS soil_moisture, ",
           "Press_", chamber.id, " AS differential_pressure, ",
           "Ground_press_hPa AS barometric_pressure ",
           "FROM raw_soil ",
           "WHERE time >= \'", start.time, "\' AND ",
           "time <= \'", end.time, "\';" )
  },
  
  stem = function(chamber.id, start.time, end.time){
    # Make a query string that will be sent to InfluxDB to retrieve the 
    # meteorological measurement data for a stem chamber. 
    
    chamber.id %<>% gsub("K", "k", .) 
    start.time %<>% as.character
    end.time %<>% as.character
    
    paste0("SELECT Temp_", chamber.id, " AS air_temperature, ",
           "Hum_", chamber.id, " AS relative_humidity, ",
           "CO2_", chamber.id, " AS stem_CO2_concentration, ",
           "Ground_press_hPa AS barometric_pressure ",
           "FROM raw_stem ",
           "WHERE time >= \'", start.time, "\' AND ",
           "time <= \'", end.time, "\';" )
  }
)