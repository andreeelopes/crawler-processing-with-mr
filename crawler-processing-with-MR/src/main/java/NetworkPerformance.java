import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.*;

import java.io.IOException;
import java.net.URL;


public class NetworkPerformance {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {
        private Text site = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont)
                throws IOException, InterruptedException {

            WarcRecord val = value.getRecord();

            String bytesString = val.getHeaderMetadataItem("Content-Length");

            String warcDate = val.getHeaderMetadataItem("WARC-Date");
            int extractionTime = 0;

            String url = val.getHeaderMetadataItem("WARC-Target-URI");

            try {
                site.set(new URL(url).getHost());
                cont.write(site, new Text(bytesString + ":" + extractionTime));

            } catch (Exception e) {
            }
        }

    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        private WritableNetPerformReduce result = new WritableNetPerformReduce();

        public void reduce(Text key, Iterable<Text> values, Context cont)
                throws IOException, InterruptedException {

            long sumNB = 0;
            long sumT = 0;
            for (Text val : values) {

                String[] splitted = val.toString().split(":");
                int numberOfBytes = Integer.valueOf(splitted[0]);
                int extractionTime = Integer.valueOf(splitted[1]);

                sumNB += numberOfBytes;
                sumT += extractionTime;
            }

            double bandwidth = sumNB / (double) sumT;


            cont.write(key, new Text(sumNB + ":" + sumT));
        }

    }


    public static void main(String[] args) throws Exception {
        Job conf = Job.getInstance(new Configuration(), "network-performance");
        conf.setJarByClass(NetworkPerformance.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MyMap.class);
        conf.setCombinerClass(MyReduce.class);
        conf.setReducerClass(MyReduce.class);

        conf.setInputFormatClass(WarcFileInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.waitForCompletion(true); // submit and wait
    }


}
