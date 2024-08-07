#!/bin/bash

hadoop jar YearlyData.jar YearlyData \
    /user/hadoop/cov19_data/sample/RAW_global_confirmed_cases_sample.csv \
    /user/hadoop/cov19_data/cleaned/agg_yearly_global_confirmed_cases_sample.csv

hadoop jar MonthlyData.jar MonthlyData \
    /user/hadoop/cov19_data/sample/RAW_global_confirmed_cases_sample.csv \
    /user/hadoop/cov19_data/agg_monthly_global_confirmed_cases_sample.csv
