#!/bin/bash

docker exec -it cov19-namenode bash
mkdir CovidDataProcessing
cd CovidDataProcessing
# # Assuming Java and Hadoop libraries are available on the container
# javac -classpath $(hadoop classpath) -d . CovidDataProcessing.java
# jar -cvf CovidDataProcessing.jar -C . .
# exit

# docker exec -it cov19-namenode hadoop jar /CovidDataProcessing/CovidDataProcessing.jar CovidDataProcessing /user/hadoop/covid_data/CONVENIENT_us_confirmed_cases.csv /user/hadoop/covid_data/output_confirmed_cases
# docker exec -it cov19-namenode hadoop jar /CovidDataProcessing/CovidDataProcessing.jar CovidDataProcessing /user/hadoop/covid_data/CONVENIENT_us_deaths.csv /user/hadoop/covid_data/output_deaths
