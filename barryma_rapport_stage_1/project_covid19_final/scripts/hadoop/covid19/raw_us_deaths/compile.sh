#!/bin/bash

# Compile Java files for Hadoop MapReduce jobs
javac -classpath `hadoop classpath` -d . USDeathsYearlyMapper.java USDeathsYearlyReducer.java USDeathsYearlyDriver.java
javac -classpath `hadoop classpath` -d . USDeathsMonthlyMapper.java USDeathsMonthlyReducer.java USDeathsMonthlyDriver.java
