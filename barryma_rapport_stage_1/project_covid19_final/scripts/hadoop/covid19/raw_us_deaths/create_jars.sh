#!/bin/bash

# Create JAR files for Hadoop MapReduce jobs
jar -cvf USDeathsYearly.jar USDeathsYearlyMapper.class USDeathsYearlyReducer.class USDeathsYearlyDriver.class
jar -cvf USDeathsMonthly.jar USDeathsMonthlyMapper.class USDeathsMonthlyReducer.class USDeathsMonthlyDriver.class
