#!/usr/bin/Rscript
# !diagnostics off
library(data.table)
library(tidyverse)
library(magrittr)

##################################################################################################
##################################################################################################
#################################### Time-offset functions #######################################
##################################################################################################
##################################################################################################
base.offset <- 50L
calvin.offset.before.sep.10th <- 420L
calvin.offset.after.sep.10th <- 240L

add_time_offset <- function(offset){
  function(time.stamp){
    time.stamp %>% as.POSIXct() %>% add(offset) %>% as.character()
  }
}

add_base_offset <- add_time_offset(base.offset)
add_calvin_offset_before_sep_10th <- add_time_offset(calvin.offset.before.sep.10th)
add_calvin_offset_after_sep_10th <- add_time_offset(calvin.offset.after.sep.10th)


add_calvin_offset <-  function(time.stamp){
  # This function deals with a jump in the clock at '2020-09-10 07:46'. Due to the 
  # chamber clock running 3 minutes late, the offset had to be increased by 180 seconds
  # before the cut-off time. Please note that we actually change the timestamps for Calvin
  # although in reality the time in the base dataset was off. However, for most
  # intents and purposes that should not matter much. 
  cut.off <- as.POSIXct('2020-09-10 07:46', format='%F %R', tz='UTC')
  fifelse(time.stamp <= cut.off, 
          add_calvin_offset_before_sep_10th(time.stamp),
          add_calvin_offset_after_sep_10th(time.stamp))
}

subtract_base_offset <- add_time_offset(-base.offset)
subtract_calvin_offset_before_sep_10th <- add_time_offset(-calvin.offset.before.sep.10th)
subtract_calvin_offset_after_sep_10th <- add_time_offset(-calvin.offset.after.sep.10th)


subtract_calvin_offset <-  function(time.stamp){
  # This is the reverse function for the application of the time-offset for Calvin.
  cut.off <- as.POSIXct('2020-09-10 07:50', format='%F %R', tz='UTC')
  fifelse(time.stamp <= cut.off, 
          subtract_calvin_offset_before_sep_10th(time.stamp),
          subtract_calvin_offset_after_sep_10th(time.stamp))
}


##################################################################################################
##################################################################################################
#################################### Various functions ###########################################
##################################################################################################
##################################################################################################



chamber_type_from_id <- function(id.string){
  # Determine chamber type from chamber id.
  # Return either "branch", "soil" or "stem" for a match and NULL otherwise.
  
  # Make a named vector as a hash table.
  chamber.type.lookup <- setNames(c("branch", "soil", "stem"), c("AK", "BK", "SK"))
  
  # Convert id string to upper case and test if it matches pattern.
  matches <- grepl("^[ABS]K[1-8]$", toupper(id.string))
  if(!matches){return(NULL)}
  
  chamber.type <- sub("^([ABS]K)[1-8]$", "\\1", id.string)
  chamber.type.lookup[chamber.type] %>% unname()
}



stopifnot(exprs={
  is.null(chamber_type_from_id("BK77"))
  is.null(chamber_type_from_id("AB7"))
  is.null(chamber_type_from_id("ZK1"))
  is.null(chamber_type_from_id("random"))
  identical(chamber_type_from_id("AK1"), "branch")
  identical(chamber_type_from_id("BK1"), "soil")
  identical(chamber_type_from_id("SK1"), "stem")
})


segregate_into_periods <- function(dt, time.gap=NULL){
  # This function checks whether two measurement
  # periods are separated by more than a predetermined
  # time. If so the two periods are treated as separate.
  # Otherwise they are labelled the as the same measurement.
  if(is.null(dt)){
    stop("segregate_into_periods: The input data is NULL!")}
  if(identical(dt[, .N], 0L)){
    stop("segregate_into_periods: The input datatable has zero rows!")}
  
  time.gap = time.gap %||% 30
  dt[, period.no:= time %>% 
       as.numeric() %>% 
       diff() %>% 
       is_weakly_greater_than(time.gap) %>% 
       cumsum() %>% c(0, .) %>% 
       as.integer()]
  
  return(dt)
}


clean_background_periods <- function(dt){
  # This function checks the length of a (background) measurement period
  # and removes any periods that have less than 85 or more than 95 rows.
  # The remaining measurement periods have 5 rows removed at the beginning
  # and end before being returned.
  dt <- dt[, if(.N>85 && .N<95) .SD, by=period.no]
  
  if(identical(dt[, .N], 0L)){return(NULL)}
  
  inner_func <- function(dt){
    n <- nrow(dt)
    dt[!c(seq(1,5), seq(n-4,n)),]
  }
  
  dt <- dt[, inner_func(.SD), by=period.no] 
  
  return(dt)
}


condense_periods <- function(dt){
  
  if(is.null(dt)){return(NULL)}
  # This function takes the timeline and only returns the first and last time value.
  dt <- dt[,.(start.time=first(time), end.time=last(time)), by=period.no]
  dt[, period.no:=order(period.no)]
}


remove_failed_backgrounds <- function(dt){
  
  if(is.null(dt)){return(NULL)}
  # This function removes calibrations where there is an insufficient number
  # of calibration values avaialble. 
  dt <- dt[licor.CO2.values>=60 & 
             calvin.CO2.values>=10 &
             calvin.O2.values>=10]
  # Remove the columns that counted the number of values recorded during 
  # a measurement period. 
  dt[, `:=`(licor.CO2.values = NULL, 
            calvin.CO2.values = NULL, 
            calvin.O2.values = NULL)]
  return(dt)
}


add_previous_period_nos_to_chamber_data <- function(Lst, dt){
  
  if(identical(length(Lst), 0L)){stop("Input Lst is empty!")}
  
  # Define internal function that will be applied to each combination of list 
  # elements. 
  add_to_period_no <- function(dt, number){
    if(is.null(dt) || identical(dt[, .N], 0L)){return(NULL)}
    dt[, period.no:=as.integer(period.no+number)]
    return(dt)
  }
  
  numbers <- setNames(dt$period.no, dt$measurement.type)
  
  chamber.types <- names(Lst)
  Lst <- lapply(chamber.types, 
                function(x){add_to_period_no(Lst[[x]], numbers[x])})
  names(Lst) <- chamber.types
  
  return(Lst)
}


ensure_type_safety <- function(dt){
  # This function sets all columns, except "time", "id", and "period.no" to double precision
  # floats. "id" is coerced to character and "period.no" to integer". 
  # This is necessary as InfluxDB throws an error when the data type of new data
  # does not conform to the existing column's data type it is meant to be written to. 
  float.cols <- setdiff(names(dt), c("time", "period.no", "id"))
  dt[, (float.cols) := lapply(.SD, as.numeric), .SDcols = float.cols]
  dt[, period.no:=as.integer(period.no)]
  dt[, id:=as.character(id)]
  return(dt)
}