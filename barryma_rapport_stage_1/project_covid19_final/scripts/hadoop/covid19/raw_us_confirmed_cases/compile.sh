#!/bin/bash

# Compile Java files for Hadoop MapReduce jobs
javac -classpath `hadoop classpath` -d . USConfirmedCasesYearlyMapper.java USConfirmedCasesYearlyReducer.java USConfirmedCasesYearlyDriver.java
javac -classpath `hadoop classpath` -d . USConfirmedCasesMonthlyMapper.java USConfirmedCasesMonthlyReducer.java USConfirmedCasesMonthlyDriver.java
