#!/bin/bash


docker service scale hadoop_stack_worker-datanode1=3
docker service scale hadoop_stack_worker-datanode2=3
