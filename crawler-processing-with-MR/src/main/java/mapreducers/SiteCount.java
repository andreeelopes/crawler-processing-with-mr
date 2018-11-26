package mapreducers; /**
 * mapReducers.SiteCount.java - counts the number of pages each site has (the number
 * of times each hostname appears at document header)
 * usage example of edu.cmu.lemurproject.* InputFormat and RecordReader
 * Vitor Duarte, FCT/NOVA
 * based on public mapReducers.WordCount examples
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import utils.WarcFileInputFormat;
import utils.WarcRecord;
import utils.WritableWarcRecord;

import java.net.URL;


public class SiteCount {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont)
                throws IOException, InterruptedException {

            WarcRecord val = value.getRecord();
            String url = val.getHeaderMetadataItem("WARC-Target-URI");
            try {
                word.set(new URL(url).getHost());
                cont.write(word, one);
            } catch (Exception e) {
            }
        }

    }


    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context cont)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            cont.write(key, result);
        }

    }


    public static void main(String[] args) throws Exception {
        Job conf = Job.getInstance(new Configuration(), "sitecount");
        conf.setJarByClass(SiteCount.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

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
