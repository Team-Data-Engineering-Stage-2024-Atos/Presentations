#!/bin/bash

javac -classpath `hadoop classpath` -d . YearlyMapper.java YearlyReducer.java YearlyData.java
javac -classpath `hadoop classpath` -d . MonthlyMapper.java MonthlyReducer.java MonthlyData.java