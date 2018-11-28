#!/usr/bin/env bash
outputDir=$1

mkdir -p ./tests/test-results/$outputDir/

for sampleSize in 1 4 8 16 32
do
    for run in $(seq 1 3)
    do
        echo ">>>Run " $run " with sample size " $sampleSize "has started."
        yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar TopWordFrequency /tests/input-data/$sampleSize/* /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
        echo ">>>Run " $run " with sample size " $sampleSize "done."
    done
done
echo "Tests concluded."