#!/usr/bin/env bash

printf ">>> Creating directories ... "
hdfs dfs -mkdir /tests
hdfs dfs -mkdir -p /tests/input-data
hdfs dfs -mkdir -p /tests/mr-results
printf "done.\n"

printf ">>> Downloading data ... "
wget -O ./sample1.warc.wet.gz "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2018-47/segments/1542039741016.16/warc/CC-MAIN-20181112172845-20181112194415-00012.warc.gz"
printf "done.\n"

printf ">>> Extracting data ... "
gzip -d ./sample1.warc.wet.gz
printf "done.\n"

printf ">>> Uploading data to hdfs ... "
hdfs dfs -put sample1.warc.wet /tests/input-data
printf "done.\n"

for i in $(seq 2 32); do
    printf ">>> Making hdfs copy 'sample${i}.warc.wet' ... "
    hdfs dfs -cp /tests/input-data/sample1.warc.wet /tests/input-data/sample${i}.warc.wet
    printf "done.\n"
done
