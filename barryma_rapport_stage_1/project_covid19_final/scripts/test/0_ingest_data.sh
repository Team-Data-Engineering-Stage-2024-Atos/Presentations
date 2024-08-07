#!/bin/bash

docker exec -it cov19-namenode hdfs dfs -mkdir -p /user/hadoop/covid_data
docker cp data/sample/CONVENIENT_us_confirmed_cases_sample.csv cov19-namenode:/CONVENIENT_us_confirmed_cases_sample.csv
docker cp data/sample/CONVENIENT_us_deaths_sample.csv cov19-namenode:/CONVENIENT_us_deaths_sample.csv

docker exec -it cov19-namenode hdfs dfs -put /CONVENIENT_us_confirmed_cases_sample.csv /user/hadoop/covid_data/
docker exec -it cov19-namenode hdfs dfs -put /CONVENIENT_us_deaths_sample.csv /user/hadoop/covid_data/
