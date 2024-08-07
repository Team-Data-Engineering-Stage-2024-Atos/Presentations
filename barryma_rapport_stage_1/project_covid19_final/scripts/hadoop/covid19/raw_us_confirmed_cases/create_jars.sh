#!/bin/bash

# Create JAR files for Hadoop MapReduce jobs
jar -cvf USConfirmedCasesYearly.jar USConfirmedCasesYearlyMapper.class USConfirmedCasesYearlyReducer.class USConfirmedCasesYearlyDriver.class
jar -cvf USConfirmedCasesMonthly.jar USConfirmedCasesMonthlyMapper.class USConfirmedCasesMonthlyReducer.class USConfirmedCasesMonthlyDriver.class
