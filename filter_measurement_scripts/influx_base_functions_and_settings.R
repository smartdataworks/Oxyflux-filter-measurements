#!/usr/bin/Rscript
# !diagnostics off

library(ConfigParser)

# Read configuration file
config <- ConfigParser$new(Sys.getenv(), optionxform=identity)
config$read('oxyflux_etl.cfg')

influxdb.client  <- influx_client(host = config$get('HOST', NA, 'INFLUX'),
                                  port = config$getfloat('PORT', NA, 'INFLUX'),
                                  user = config$get('USER_NAME', NA, 'INFLUX'),
                                  pass = config$get('PASSWORD', NA, 'INFLUX'),
                                  chunk_size = 10000L,
                                  post_chunk_size = 1000L,
                                  ssl = FALSE)


oxyflux_query <- function(query){
  # This function will provide most of the settings and
  # only takes the actual query as an input.
  influx_query(client=influxdb.client,
               query= query,
               database=config$get('DB_NAME', NA, 'INFLUX'),
               precision='s',
               verbose=FALSE)
}


oxyflux_write <- function(df, measurement, time.column, tags=NULL){
  # This function will provide most of the settings and 
  # only takes the minimum number of inputs.
  influx_write(client=influxdb.client,
               x = df,
               measurement=measurement,
               database=config$get('DB_NAME', NA, 'INFLUX'),
               precision='s',
               tags=tags,
               timestamp=time.column,
               tz='UTC')
}