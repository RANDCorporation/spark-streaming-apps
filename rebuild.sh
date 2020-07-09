#!/bin/bash

# clean install
# ./copy_libs.sh

rm ./lib/desh-streaming-job-assembly-1.0.jar
sbt clean && sbt assembly
cp target/scala-2.10/desh-streaming-job-assembly-1.0.jar ./lib/

