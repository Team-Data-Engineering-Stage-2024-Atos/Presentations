#!/bin/bash
header_line="Province_State,Admin2,UID,iso2,iso3,code3,FIPS,Country_Region,Lat,Long_,Combined_Key,Population,1/22/20,1/23/20,..."

# Run the Hadoop MapReduce job for yearly aggregation
hadoop jar USDeathsYearly.jar USDeathsYearlyDriver \
    /user/hadoop/cov19_data/sample/RAW_us_deaths_sample.csv \
    /user/hadoop/cov19_data/cleaned/agg_monthly_us_deaths_sample.csv "$header_line"