#!/bin/bash

mapreduce_input=$1
hive_input=$2
hive_output=$3

if hadoop fs -test -d $hive_output; then
    hadoop fs -rm -r $hive_output
fi


beeline -n "$(id -un)" -u jdbc:hive2://localhost:10000 --hivevar mapreduce_input=$mapreduce_input --hivevar hive_input=$hive_input --hivevar hive_output=$hive_output -f carrentals.hql