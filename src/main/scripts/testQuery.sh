#!/usr/bin/env bash

outputDir=$1

mkdir -p ./tests/test-results/$outputDir/

sampleSize=1
for run in $(seq 1 3); do
    echo ">>> Run " $run " with sample size " $sampleSize "has started."
    yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar NetworkPerformanceProcessing \
        /tests/input-data/sample1.warc.wet                                                  \
        /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
    echo ">>> Run " $run " with sample size " $sampleSize "done."
done

sampleSize=2
for run in $(seq 1 3); do
    echo ">>> Run " $run " with sample size " $sampleSize "has started."
    yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar NetworkPerformanceProcessing \
        /tests/input-data/sample1.warc.wet                                                  \
        /tests/input-data/sample2.warc.wet                                                  \
        /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
    echo ">>> Run " $run " with sample size " $sampleSize "done."
done

sampleSize=4
for run in $(seq 1 3); do
    echo ">>> Run " $run " with sample size " $sampleSize "has started."
    yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar NetworkPerformanceProcessing \
        /tests/input-data/sample1.warc.wet                                                  \
        /tests/input-data/sample2.warc.wet                                                  \
        /tests/input-data/sample3.warc.wet                                                  \
        /tests/input-data/sample4.warc.wet                                                  \
        /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
    echo ">>> Run " $run " with sample size " $sampleSize "done."
done

sampleSize=8
for run in $(seq 1 3); do
    echo ">>> Run " $run " with sample size " $sampleSize "has started."
    yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar NetworkPerformanceProcessing \
        /tests/input-data/sample1.warc.wet                                                  \
        /tests/input-data/sample2.warc.wet                                                  \
        /tests/input-data/sample3.warc.wet                                                  \
        /tests/input-data/sample4.warc.wet                                                  \
        /tests/input-data/sample5.warc.wet                                                  \
        /tests/input-data/sample6.warc.wet                                                  \
        /tests/input-data/sample7.warc.wet                                                  \
        /tests/input-data/sample8.warc.wet                                                  \
        /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
    echo ">>> Run " $run " with sample size " $sampleSize "done."
done

sampleSize=16
for run in $(seq 1 3); do
    echo ">>> Run " $run " with sample size " $sampleSize "has started."
    yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar NetworkPerformanceProcessing \
        /tests/input-data/sample1.warc.wet                                                  \
        /tests/input-data/sample2.warc.wet                                                  \
        /tests/input-data/sample3.warc.wet                                                  \
        /tests/input-data/sample4.warc.wet                                                  \
        /tests/input-data/sample5.warc.wet                                                  \
        /tests/input-data/sample6.warc.wet                                                  \
        /tests/input-data/sample7.warc.wet                                                  \
        /tests/input-data/sample8.warc.wet                                                  \
        /tests/input-data/sample9.warc.wet                                                  \
        /tests/input-data/sample10.warc.wet                                                 \
        /tests/input-data/sample11.warc.wet                                                 \
        /tests/input-data/sample12.warc.wet                                                 \
        /tests/input-data/sample13.warc.wet                                                 \
        /tests/input-data/sample14.warc.wet                                                 \
        /tests/input-data/sample15.warc.wet                                                 \
        /tests/input-data/sample16.warc.wet                                                 \
        /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
    echo ">>> Run " $run " with sample size " $sampleSize "done."
done

sampleSize=32
for run in $(seq 1 3); do
    echo ">>> Run " $run " with sample size " $sampleSize "has started."
    yarn jar tests/crawler-processing-with-MR-1.0-SNAPSHOT.jar NetworkPerformanceProcessing \
        /tests/input-data/sample1.warc.wet                                                  \
        /tests/input-data/sample2.warc.wet                                                  \
        /tests/input-data/sample3.warc.wet                                                  \
        /tests/input-data/sample4.warc.wet                                                  \
        /tests/input-data/sample5.warc.wet                                                  \
        /tests/input-data/sample6.warc.wet                                                  \
        /tests/input-data/sample7.warc.wet                                                  \
        /tests/input-data/sample8.warc.wet                                                  \
        /tests/input-data/sample9.warc.wet                                                  \
        /tests/input-data/sample10.warc.wet                                                 \
        /tests/input-data/sample11.warc.wet                                                 \
        /tests/input-data/sample12.warc.wet                                                 \
        /tests/input-data/sample13.warc.wet                                                 \
        /tests/input-data/sample14.warc.wet                                                 \
        /tests/input-data/sample15.warc.wet                                                 \
        /tests/input-data/sample16.warc.wet                                                 \
        /tests/input-data/sample17.warc.wet                                                 \
        /tests/input-data/sample18.warc.wet                                                 \
        /tests/input-data/sample19.warc.wet                                                 \
        /tests/input-data/sample20.warc.wet                                                 \
        /tests/input-data/sample21.warc.wet                                                 \
        /tests/input-data/sample22.warc.wet                                                 \
        /tests/input-data/sample23.warc.wet                                                 \
        /tests/input-data/sample24.warc.wet                                                 \
        /tests/input-data/sample25.warc.wet                                                 \
        /tests/input-data/sample26.warc.wet                                                 \
        /tests/input-data/sample27.warc.wet                                                 \
        /tests/input-data/sample28.warc.wet                                                 \
        /tests/input-data/sample29.warc.wet                                                 \
        /tests/input-data/sample30.warc.wet                                                 \
        /tests/input-data/sample31.warc.wet                                                 \
        /tests/input-data/sample32.warc.wet                                                 \
        /tests/mr-results/$outputDir/size-$sampleSize/run-$run >> tests/test-results/$outputDir/results-$sampleSize-temp.txt && tail -n 1 tests/test-results/$outputDir/results-$sampleSize-temp.txt >> tests/test-results/$outputDir/results-$sampleSize.txt && rm tests/test-results/$outputDir/results-$sampleSize-temp.txt
    echo ">>> Run " $run " with sample size " $sampleSize "done."
done

sleep 3s
tar -czf "scc${outputDir}.tar.gz" "./tests/test-results/${outputDir}/"

echo "Tests concluded."
