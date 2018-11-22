import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import utils.WarcFileInputFormat;

public class Top10LargestSites {

    public static class MyMap extends Mapper<Text, WritableNetPerformReduce, NullWritable, WritableTop10LargestMap> {

        private static TreeMap<Unique, WritableTop10LargestMap> toRecordMap =
                new TreeMap<Unique, WritableTop10LargestMap>(new UniqueComparator());


        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(Text key, WritableNetPerformReduce value, Context cont)
                throws IOException, InterruptedException {

            toRecordMap.put(new Unique(value.getNumberOfBytes().get()), new WritableTop10LargestMap(key, value));

            //Limit map to 10 entries
            Iterator<Map.Entry<Unique, WritableTop10LargestMap>> iter = toRecordMap.entrySet().iterator();

            while (toRecordMap.size() > 10) {
                iter.next();
                iter.remove();
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            // Output our ten records to the reducers with a null key
            for (WritableTop10LargestMap t : toRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }

        }


    }


    public static class MyReduce extends Reducer<NullWritable, WritableTop10LargestMap, Text, LongWritable> {

        private static TreeMap<Unique, WritableTop10LargestMap> toRecordMap =
                new TreeMap<Unique, WritableTop10LargestMap>(new UniqueComparator());


        public void reduce(NullWritable key, Iterable<WritableTop10LargestMap> values, Context context)
                throws IOException, InterruptedException {


            for (WritableTop10LargestMap value : values) {
                toRecordMap.put(new Unique(value.getNumberOfBytes().get()), value);
            }


            // If we have more than ten records, remove the one with the lowest number of bytes
            // As this tree map is sorted in descending order, the site with
            // the lowest number of bytes is the last key.
            Iterator<Map.Entry<Unique, WritableTop10LargestMap>> iter = toRecordMap.entrySet().iterator();
            while (toRecordMap.size() > 10) {
                iter.next();
                iter.remove();
            }


            Map<Unique, WritableTop10LargestMap> newMap = new TreeMap<Unique, WritableTop10LargestMap>(Collections.reverseOrder());
            newMap.putAll(toRecordMap);

            for (WritableTop10LargestMap t : newMap.values()) {
                // Output our ten records to the file system with a null key
                context.write(t.getSiteURI(), t.getNumberOfBytes());
            }

        }

    }


    public static void main(String[] args) throws Exception {

        //filter out non latin sites


        //bytes of each site


        //top10 largest sites

        Job conf = Job.getInstance(new Configuration(), "top10largest");
        conf.setJarByClass(SiteCount.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(MyMap.class);
        conf.setCombinerClass(MyReduce.class);
        conf.setReducerClass(MyReduce.class);

        conf.setInputFormatClass(WarcFileInputFormat.class); // TODO
        conf.setOutputFormatClass(TextOutputFormat.class); // TODO

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.waitForCompletion(true); // submit and wait

        // Top10PopularWords


    }

}