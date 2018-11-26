package mapreducers;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.*;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class NetworkPerformance {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {
        private Text site = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont) {

            WarcRecord val = value.getRecord();

            String bytesString = val.getHeaderMetadataItem("Content-Length");
            String warcDate = val.getHeaderMetadataItem("WARC-Date");
            String url = val.getHeaderMetadataItem("WARC-Target-URI");

            try {
                site.set(new URL(url).getHost());
                cont.write(site, new Text(bytesString + ":" + warcDate));
            } catch (Exception e) {
            }
        }

    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        private Text value = new Text();
        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

        public void reduce(Text key, Iterable<Text> values, Context cont)
                throws IOException, InterruptedException {

            TreeSet<Calendar> extractionDates = new TreeSet<Calendar>();

            long sumNB = 0;
            for (Text val : values) {
                String[] splittedVal = val.toString().split(":", 2);
                long numberOfBytes = Long.valueOf(splittedVal[0]);
                String warcDate = splittedVal[1];

                extractionDates.add(parseDate(warcDate));
                sumNB += numberOfBytes;
            }
//
//            if (extractionDates.size() > 1) {
//
//                System.out.println();
//                System.out.println(sumNB);
//                System.out.println(extractionDates.first().getTime());
//                System.out.println(extractionDates.last().getTime());
//                System.out.println();
//            }

            long extractionTime = extractionDates.size() > 1 ?
                    getIntervalInSecs(extractionDates.first(), extractionDates.last()) : -1;


            String bandwidth = "NA";
            String extractionTimeString = "NA";
            if (extractionTime != -1) {
                bandwidth = String.valueOf(sumNB / extractionTime);
                extractionTimeString = String.valueOf(extractionTime);
            }

            value.set(sumNB + ":" + extractionTimeString + ":" + bandwidth); //TODO bandwith

            cont.write(key, value);
        }

        private long getIntervalInSecs(Calendar first, Calendar last) {
            return (last.getTimeInMillis() - first.getTimeInMillis()) / 1000;
        }

        private Calendar parseDate(String warcDate) {
            try {
                return DateUtils.toCalendar(formatter.parse(warcDate.replaceAll("Z$", "+0000")));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return null;
        }

    }

}
