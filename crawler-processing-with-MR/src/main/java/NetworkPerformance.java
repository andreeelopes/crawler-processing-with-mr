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

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, WritableNBT> {
        private Text site = new Text();
        private WritableNBT mapValue = new WritableNBT();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont)
                throws IOException, InterruptedException {

            WarcRecord val = value.getRecord();

            String bytesString = val.getHeaderMetadataItem("Content-Length");
            LongWritable numberOfBytes = new LongWritable(Integer.valueOf(bytesString));

            Text warcDate = new Text(val.getHeaderMetadataItem("WARC-Date"));
            LongWritable extractionTime = new LongWritable(0);

            mapValue.setExtractionTime(extractionTime);
            mapValue.setNumberOfBytes(numberOfBytes);

            String url = val.getHeaderMetadataItem("WARC-Target-URI");

            try {
                site.set(new URL(url).getHost());
                cont.write(site, mapValue);

            } catch (Exception e) {
            }
        }

    }


    public static class MyReduce extends Reducer<Text, WritableNBT, Text, WritableNBTReduce> {

        private WritableNBTReduce result = new WritableNBTReduce();

        public void reduce(Text key, Iterable<WritableNBT> values, Context cont)
                throws IOException, InterruptedException {

            long sumNB = 0;
            long sumT = 0;
            for (WritableNBT val : values) {
                sumNB += val.getNumberOfBytes().get();
                sumT += val.getExtractionTime().get();
            }

            double bandwidth = sumNB / (double) sumT;

            result.setNumberOfBytes(new LongWritable(sumNB));
            result.setNumberOfBytes(new LongWritable(sumT));
            result.setBandwith(new DoubleWritable(bandwidth));


            cont.write(key, result);
        }

    }


    public static void main(String[] args) throws Exception {
        Job conf = Job.getInstance(new Configuration(), "network-performance");
        conf.setJarByClass(NetworkPerformance.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(WritableNBTReduce.class);

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
