#!/bin/bash

input_dir1=$1
output_dir3=$2

if hadoop fs -test -d $input_dir1; then
    echo "Input directory exists: $input_dir1"
else
    echo "Input directory does not exist: $input_dir1"
    exit 1
fi

if hadoop fs -test -d $output_dir3; then
    hadoop fs -rm -r $output_dir3
fi

hadoop jar carrentals.jar $input_dir1 $output_dir3

exit 0