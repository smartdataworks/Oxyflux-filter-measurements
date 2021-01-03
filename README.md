# Oxyflux filter-measurements

This repositiory contains the files necessary to filter out and combine the chamber measurement data from the continuous sensor streams on a local or remote InfluxDB server.

## Options

The script can be run from terminal simply by calling `filter_measurement_periods.R`. It can also be called through a cron job to enable automatic processing at regular intervals.

## Usage

```bash
Rscript filter_measurement_periods.R
```

## Compatability

The script was written for R 3.63 and does not contain any code or packages that are known to have breaking changes coming soon (as of the time of this writing).

## Configuration

Configuration is done in a local cfg-file. This way folder structures, database access and--most importantly--passwords are not hardcoded into the script. An empty cfg-config file is supplied with the repository.

## Details

The script finds the time periods when an actual chamber or background measurement took place. It combines these data from both the data streams containing the gas concentration data and the chamber sensor data and applies the appropriate time-offset. The remaining data has already some quality checks applied to it.

## License

If parts of this code are useful to you please feel free to copy it.  It is free software, and may be redistributed under the terms specified in the [LICENSE](LICENSE.txt) file.
