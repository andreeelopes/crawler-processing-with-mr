#!/usr/bin/env bash
outputDir=$1

for sampleSize in 1
do
    for run in $(seq 1 3)
    do
        yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar TopWordFrequency /tests/input-data/$sampleSize/* /tests/mr-results/$outputDir/t-$run && tail -1 >> tests/test-results/$outputDir/results-$sampleSize.txt
    done
done
echo "Tests concluded."

#$sampleSize=$0
#find /tests/input-data/* |head -$sampleSize

# a) um ficheiro; b) quatro
#ficheiros; c) oito ficheiros; d) 16 ficheiros.

