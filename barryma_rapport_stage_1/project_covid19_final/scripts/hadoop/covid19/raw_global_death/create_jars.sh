#!/bin/bash

jar -cvf YearlyData.jar YearlyMapper.class YearlyReducer.class YearlyData.class
jar -cvf MonthlyData.jar MonthlyMapper.class MonthlyReducer.class MonthlyData.class