#!/usr/bin/Rscript
# !diagnostics off

library(influxR)


source("filter_measurement_scripts/influx_base_functions_and_settings.R")
source("filter_measurement_scripts/query_text_generators.R")


# switch.columns <- setNames(c(paste0("AK", 1:4), 
#                              "Buffer", 
#                              paste0("BK", 1:8), 
#                              "CylAir", 
#                              paste0("SK", 1:4)), 
#                            c(paste("branch.chamber", 1:4, sep="."),
#                              "buffer",
#                              paste("soil.chamber", 1:8, sep="."),
#                              "cylinder",
#                              paste("stem.chamber", 1:4, sep=".")))



##################################################################################################
##################################################################################################
################################## Read data from InfluxDB ######################################
##################################################################################################
##################################################################################################

# check_measurements <- function(){
#   dt <- oxyflux_query(measurement_names_query())
#   get_measurements(influxdb.client, "db_OxyFlux_new_system")
# }


get_new_start_times_and_period_nos <- function(){
  
  # This function returns a data table containing the last measurement
  # times and period numbers for chambers. When the measurement table is 
  # missing it will handle this gracefully and return "1" for measurement
  # period number and midnight 12th May 2020 for start.date
  
  inner_func <- function(measurement.type, measurement.name){
    query.results <- measurement.name %>% 
      previous_measurement_period_query() %>% 
      oxyflux_query()
    
    data.table(measurement.type=measurement.type,
               period.no=as.integer(query.results$period_no %||% 1L),
               time=query.results$time %||% "2020-05-12 00:00:00")
  }
  
  measurement.names <- c(branch="branch_chamber_measurements",
                         soil="soil_chamber_measurements",
                         stem="stem_chamber_measurements")

  dt <- data.table(measurement.type=names(measurement.names),
                   measurement.name=measurement.names)
  
  Lst <- split(dt, dt$measurement.type)
  
  dt <- lapply(Lst, function(dt){inner_func(dt$measurement.type,
                                            dt$measurement.name)}) %>% do.call(rbind, .)
}


get_new_background_periods <- function(background.id, start.time=NULL){
  # This function is an aggregation of the previously defined functions
  # that are called one by one. 
  
  last.overall <- oxyflux_query(last_measurement_query(background.id))
  last.time.stamp <- last.overall$time - 600                             
  last.inactive <- oxyflux_query(last_non_background_measurement_query(
    background.id, 
    last.time.stamp))
  last.inactive.time <- last.inactive$time
  
  # Construct and run query to get all completed measurements since the last query.
  last.active <- active_measurement_query(background.id=background.id,
                                          start.time=start.time,
                                          end.time=last.inactive.time)
  dt <- oxyflux_query(last.active)
  
  # Set time column as data.table key.
  dt[, id:=NULL]
  setkey(dt, time)
  
  dt %<>%  segregate_into_periods() %>%  
    clean_background_periods() %>%  
    condense_periods()
  
  return(dt)
}


filter_data <- function(measurement.data, filter.data, offset_function){
  # This function reads in the actual data, a mask by which the data will be filtered
  # and a time-offset that is defined by a function
  # The return value is either the filtered data table or an empty data table. 
  if(any(vapply(list(measurement.data, filter.data), 
                function(x) {is.null(x) || identical(x[, .N], 0L)},
                FUN.VALUE=logical(1)))){
    return(data.table())
  }
  
  # if(identical(measurement.data[, .N], 0L) || identical(filter.data[, .N], 0L)) {
  #   return(data.table())}

  # Make copies of data tables. This is important! Otherwise the input values will also be altered.
  measurement.data <- copy(measurement.data)
  filter.data <- copy(filter.data)
  
  filter.data[, time:=offset_function(time)]
  
  # Set the time columns of both data tables to character and then make them the primary keys.
  measurement.data$time %<>% as.POSIXct()
  filter.data$time %<>% as.POSIXct()
  setkey(measurement.data, time)
  setkey(filter.data, time)
  
  # Merge the two tables.
  output <- merge(filter.data, measurement.data, all.x=TRUE)
  # Drop the dummy column.
  output[, dummy := NULL]
  
  return(output)
}


call_background_data <- function(dt.row){
  
  if(is.null(dt.row)){return(NULL)}
  # This function calls all the remaining data points
  base.start <- add_base_offset(dt.row$start.time)
  base.end <- add_base_offset(dt.row$end.time)
  calvin.start <- add_calvin_offset(dt.row$start.time)
  calvin.end <- add_calvin_offset(dt.row$end.time)
  
  base.query <- make_background_base_query(base.start, base.end)
  calvin.query <- make_calvin_query(calvin.start, calvin.end)
  filter.query <- make_filter_query(dt.row$start.time, dt.row$end.time)
  
  # Load data from InfluxDB
  base.data <- oxyflux_query(base.query) %||% data.table()
  calvin.data <- oxyflux_query(calvin.query) %||%  data.table()
  filter.data <- oxyflux_query(filter.query) %||% data.table()
  
  # Here we filter the data for any overlapping measurement periods.
  base.data <- filter_data(base.data, filter.data, add_base_offset)
  calvin.data <- filter_data(calvin.data, filter.data, add_calvin_offset)
  
  dt.row$licor.CO2 <- base.data$base_CO2 %>% mean_() 
  dt.row$calvin.CO2 <- calvin.data$calvin_CO2 %>%  mean_()
  dt.row$calvin.O2 <- calvin.data$calvin_O2 %>% mean_()
  dt.row$licor.CO2.values <- base.data$base_CO2 %>% is.na() %>% not() %>% sum()
  dt.row$calvin.CO2.values <- calvin.data$calvin_CO2 %>% is.na() %>% not() %>% sum()
  dt.row$calvin.O2.values <- calvin.data$calvin_O2 %>% is.na() %>% not() %>% sum()
  
  return(dt.row)
}

retrieve_chamber_data <- function(chamber.id, start.time, end.time){
  # Retrieve chamber data for any of the three types of chamber.
  # The function determines the chamber type from the chamber.id.
  
  # Get the measurement periods 
  periods <- oxyflux_query(make_measurement_period_query(
    chamber.id, start.time, end.time))
  if(is.null(periods)){
    return(NULL)}
  periods %<>%  segregate_into_periods(30)
  periods[, period.no:=as.integer(period.no)]
  periods <- periods[,.(start.time=first(time), end.time=last(time)), by=period.no]
  
  # Determine the chamber type.
  chamber.type <- chamber_type_from_id(chamber.id)
  if(is.null(chamber.type)){return(NULL)}
  
  inner_func <- function(chamber.id, chamber.type, start.time, end.time){
    
    base.start.time <- add_base_offset(start.time)
    base.end.time <- add_base_offset(end.time)
    
    calvin.start.time <- add_calvin_offset(start.time)
    calvin.end.time <- add_calvin_offset(end.time)
    
    # Make the database query strings.
    base.query <- make_chamber_base_query(base.start.time, base.end.time, chamber.id)
    calvin.query <- make_calvin_query(calvin.start.time, calvin.end.time)
    filter.query <- make_filter_query(start.time, end.time)
    chamber.query <- make_chamber_sensor_queries[[chamber.type]](chamber.id, start.time, end.time)
    
    # Run queries against database.
    base.data <- oxyflux_query(base.query)
    calvin.data <- oxyflux_query(calvin.query)
    filter.data <- oxyflux_query(filter.query)
    chamber.data <- oxyflux_query(chamber.query)
    
    # Here we filter the data for any overlapping measurement periods.
    base.data <- filter_data(base.data, filter.data, add_base_offset)
    calvin.data <- filter_data(calvin.data, filter.data, add_calvin_offset)
    
    # As a precaution we put in a test for empty query outputs.
    # If any of the queries is empty the function just returns NULL.
    # print("retrieve_chamber_data: Test if chamber data exists.")
    if(any(vapply(list(base.data, calvin.data, chamber.data), is.null, FUN.VALUE=logical(1)))){
      return(NULL)
    }
    # print("retrieve_chamber_data: Test if chamber data is empty.")
    if(any(vapply(list(base.data, calvin.data, chamber.data), function(x) {identical(x[, .N], 0L)}, FUN.VALUE=logical(1)))){
      return(NULL)
    }
    
    # Adjust the time-columns for the merge step.
    base.data$time %<>% subtract_base_offset %>% as.POSIXct()
    calvin.data$time %<>% subtract_calvin_offset  %>% as.POSIXct()
    chamber.data$time %<>% as.POSIXct()
    setkey(base.data, time)
    setkey(calvin.data, time)
    setkey(chamber.data, time)
    
    # Replace all "_" with "."
    dt_col_rename(base.data, "_", "\\.")
    dt_col_rename(calvin.data, "_", "\\.")
    dt_col_rename(chamber.data, "_", "\\.")
    
    # Ensure that all columns are of the correct type.
    base.data$base.CO2 %<>% as.double()
    base.data$flow.rate %<>% as.double()
    calvin.data$calvin.CO2 %<>%  as.double()
    calvin.data$calvin.O2 %<>% as.double()
    numeric.columns <- chamber.data[, !"time"] %>%  names()
    chamber.data[, (numeric.columns) := lapply(.SD, as.double), .SDcols=numeric.columns]
    
    # Merge the data into one large data table.
    merged.data <- Reduce(function(...) merge(..., all.x=TRUE), 
                          list(base.data, calvin.data, chamber.data))

    return(merged.data)
  }
  
  periods[, period.no:=as.integer(period.no)]
  chamber.data <- periods[, inner_func(chamber.id, chamber.type, start.time, end.time), by=period.no]
  
  # Check if chamber.data actually contains anything:
  if(is.null(chamber.data)){return(NULL)}
  if(identical(chamber.data[, .N], 0L)){return(NULL)}
  
  chamber.data$secs <- chamber.data$time %>% difftime(start.time, tz="UTC", units="secs") %>% 
    as.integer()
  chamber.data$id <- chamber.id
  # Delete period.no column.
  # chamber.data[, period.no:=NULL]
  
  return(chamber.data)
}


get_branch_measurements <- function(row.1, row.2){
  
  # First, let's rearrange the data we need.
  background.period <- row.1$period.no %>%  as.integer()
  start.time <- row.1$end.time
  end.time <- row.2$start.time
  duration <- difftime(row.2$start.time, 
                       row.1$end.time, 
                       tz='UTC', 
                       units='secs') %>% as.integer()
  
  licor.CO2.lm <- lm(c(row.1$licor.CO2, row.2$licor.CO2)~c(0, duration))$coefficients
  calvin.CO2.lm <- lm(c(row.1$calvin.CO2, row.2$calvin.CO2)~c(0, duration))$coefficients
  calvin.O2.lm <- lm(c(row.1$calvin.O2, row.2$calvin.O2)~c(0, duration))$coefficients
  
  chamber.ids <- paste0("AK", 1:4)
  chamber.data <- lapply(chamber.ids, 
                         retrieve_chamber_data, 
                         start.time=start.time, 
                         end.time=end.time) %>% 
    do.call(rbind, .)
  
  if(is.null(chamber.data)){return(NULL)}
  
  chamber.data[, base.CO2.diff:=base.CO2 - (licor.CO2.lm[1] + (secs * licor.CO2.lm[2]))]
  chamber.data[, calvin.CO2.diff:=calvin.CO2 - (calvin.CO2.lm[1] + (secs * calvin.CO2.lm[2]))]
  chamber.data[, calvin.O2.diff:=calvin.O2 - (calvin.O2.lm[1] + (secs * calvin.O2.lm[2]))]
  
  chamber.data[, secs:=NULL]
  setcolorder(chamber.data, c("period.no", "id"))
  
  return(chamber.data)
}


get_soil_or_stem_measurements <- function(chamber_id_func){
  
  function(row.1, row.2){
    
    # First, let's rearrange the data we need.
    background.period <- row.1$period.no %>%  as.integer()
    start.time <- row.1$end.time
    end.time <- row.2$start.time
    
    chamber.ids <- chamber_id_func
    chamber.data <- lapply(chamber.ids, 
                           retrieve_chamber_data, 
                           start.time=start.time, 
                           end.time=end.time) %>% 
      do.call(rbind, .)
    
    if(is.null(chamber.data)){return(NULL)}
    
    chamber.data[, base.CO2.diff:=base.CO2 - row.1$licor.CO2]
    chamber.data[, calvin.CO2.diff:=calvin.CO2 - row.1$calvin.CO2]
    chamber.data[, calvin.O2.diff:=calvin.O2 - row.1$calvin.O2]
    
    chamber.data[, secs:=NULL]
    
    return(chamber.data)
  }
}

get_soil_measurements <- get_soil_or_stem_measurements({paste0("BK", 1:8)})
get_stem_measurements <- get_soil_or_stem_measurements({paste0("SK", 1:4)})


##################################################################################################
##################################################################################################
############################### Write data back to InfluxDB ######################################
##################################################################################################
##################################################################################################

write_chamber_data_to_influxdb <- function(dt, measurement){
  # Make sure that input data table is not empty. Otherwise there 
  if(is.null(dt) || identical(dt[, .N], 0L)){
    return(NULL)
  }
  
  dt <- copy(dt)
  
  # Change name separator of columns from dot to underscore.
  dt_col_rename(dt, "\\.", "_")
  
  # Split data table into a list by chamber id. 
  Lst <- split(dt, dt$id)
  # Extract the chamber id value for each list element and prepare for later use in 
  # the influxDB write statement.
  ids <- lapply(Lst, function(x){unique(x$id)[[1]]})
  ids <- lapply(ids, function(x){setNames(x, "chamber_id")})
  
  # Extract time columns to separate list.
  times <- lapply(Lst, function(x) x$time)
  
  # Remove time and id columns from data tables. 
  Lst <- lapply(Lst, function(x) x[, c("time", "id"):=NULL])
  
  # Write the data tables to InfluxDB
  mapply(oxyflux_write,
         df=Lst,
         time.column=times,
         tags=ids,
         MoreArgs = list("measurement"=measurement))
}
