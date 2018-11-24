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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import utils.WarcFileInputFormat;

public class Top10LargestSites {

    public static class MyMap extends Mapper<LongWritable, Text, NullWritable, Text> {

        private static TreeMap<Unique, String> toRecordMap =
                new TreeMap<Unique, String>(new UniqueComparator());


        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, Text value, Context cont) {

            String[] splitedValue = value.toString().split(":", 2);
            long bytes = Long.parseLong(splitedValue[0]);

            toRecordMap.put(new Unique(bytes), key.toString() + ":" + value);// [bytes, (site, bytes, content)]

            //Limit map to 10 entries
            Iterator<Map.Entry<Unique, String>> iter = toRecordMap.entrySet().iterator();

            while (toRecordMap.size() > 10) {
                iter.next();
                iter.remove();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            // Output our ten records to the reducers with a null key
            for (String val : toRecordMap.values()) {
                context.write(NullWritable.get(), new Text(val));
            }
        }
    }


    public static class MyReduce extends Reducer<NullWritable, Text, Text, Text> {

        private static TreeMap<Unique, String> toRecordMap =
                new TreeMap<Unique, String>(new UniqueComparator());


        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            for (Text val : values) {// val = (site, bytes, content)
                String[] splitedValue = val.toString().split(":", 3);
                long bytes = Long.parseLong(splitedValue[1]);

                toRecordMap.put(new Unique(bytes), val.toString());
            }

            // If we have more than ten records, remove the one with the lowest number of bytes
            // As this tree map is sorted in descending order, the site with
            // the lowest number of bytes is the last key.
            Iterator<Map.Entry<Unique, String>> iter = toRecordMap.entrySet().iterator();
            while (toRecordMap.size() > 10) {
                iter.next();
                iter.remove();
            }

            //just to reverse the order from ascending to descending
            Map<Unique, String> newMap = new TreeMap<Unique, String>(Collections.reverseOrder());
            newMap.putAll(toRecordMap);

            for (String val : newMap.values()) {
                String[] splitedValue = val.split(":", 3);
                String url = splitedValue[0];
                String content = splitedValue[2];
                // Output our ten records to the file system with a null key
                context.write(new Text(url), new Text(content));
            }
        }
    }


    public static void main(String[] args) throws Exception {

        //filter out non latin sites
        //bytes of each site
        //top10 largest sites

        Job conf = Job.getInstance(new Configuration(), "top10largest");
        conf.setJarByClass(Top10LargestSites.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MyMap.class);
        conf.setCombinerClass(MyReduce.class);
        conf.setReducerClass(MyReduce.class);

        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.waitForCompletion(true); // submit and wait

        // Top10PopularWords
    }
}