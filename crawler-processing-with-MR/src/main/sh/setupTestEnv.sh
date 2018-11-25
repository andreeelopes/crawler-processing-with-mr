#!/usr/bin/env bash

hdfs dfs -mkdir /tests

hdfs dfs -mkdir -p /tests/input-data/1
hdfs dfs -mkdir -p /tests/input-data/4
hdfs dfs -mkdir -p /tests/input-data/8
hdfs dfs -mkdir -p /tests/input-data/16

hdfs dfs -mkdir -p /tests/mr-results
hdfs dfs -mkdir -p /tests/test-results

wget -O ./sample1.warc.wet.gz "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-43/segments/1508187820466.2/wet/CC-MAIN-20171016214209-20171016234209-00000.warc.wet.gz"
gzip -d ./sample1.warc.wet.gz
hdfs dfs -put sample1.warc.wet /tests/input-data/1

for (( VAR = 1; VAR < 5; ++VAR )); do
    hdfs dfs -cp /tests/input-data/1/sample1.warc.wet /tests/input-data/4/sample$VAR.warc.wet
done

for (( VAR = 1; VAR < 9; ++VAR )); do
    hdfs dfs -cp /tests/input-data/1/sample1.warc.wet /tests/input-data/8/sample$VAR.warc.wet
done

for (( VAR = 1; VAR < 17; ++VAR )); do
    hdfs dfs -cp /tests/input-data/1/sample1.warc.wet /tests/input-data/16/sample$VAR.warc.wet
done