#!/bin/bash

# Run the Hadoop MapReduce job for monthly aggregation
hadoop jar USConfirmedCasesMonthly.jar USConfirmedCasesMonthlyDriver \
    /user/hadoop/cov19_data/sample/RAW_us_confirmed_cases_sample.csv \
    /user/hadoop/cov19_data/cleaned/agg_monthly_us_confirmed_cases_sample.csv
