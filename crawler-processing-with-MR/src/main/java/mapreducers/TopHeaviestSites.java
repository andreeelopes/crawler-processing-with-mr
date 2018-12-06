package mapreducers;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.*;

import java.io.IOException;
import java.util.*;


public class TopHeaviestSites {

    private static final int TOP_SIZE = 10;


    public static class MyMap extends Mapper<Text, Text, NullWritable, Text> {

        private static TreeMap<Unique, String> toRecordMap =
                new TreeMap<Unique, String>(new UniqueComparator());


        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        //key =  0 value =  site bytes content
        public void map(Text key, Text value, Context cont) {

            if (key.toString().length() == 0 || value.toString().length() == 0)
                return;


            String[] splitValue = value.toString().split(":", 2);
            long bytes = 0;
            bytes = Long.parseLong(splitValue[0]);


            toRecordMap.put(new Unique(bytes), key.toString() + ":" + value);// [bytes, (site, bytes, content)]

            //Limit map to TOP_SIZE entries
            Iterator<Map.Entry<Unique, String>> iter = toRecordMap.entrySet().iterator();

            while (toRecordMap.size() > TOP_SIZE) {
                iter.next();
                iter.remove();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            // Output our ten records to the reducers with a null key
            for (String val : toRecordMap.values()) {
                context.write(NullWritable.get(), new Text(val));//null, (site, bytes, content)
            }
        }
    }


    public static class MyReduce extends Reducer<NullWritable, Text, Text, Text> {

        private static TreeMap<Unique, String> toRecordMap =
                new TreeMap<Unique, String>(new UniqueComparator());//bytes, (site, bytes, content)


        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            for (Text val : values) {// val = (site, bytes, content)
                System.err.print("\n---DEBUG-Reducer---\nval=" + val.toString());
                String[] splitValue = val.toString().split(":", 3);

                String site = splitValue[0];
                long bytes = Long.parseLong(splitValue[1]);
                String content = splitValue[2];
                System.err.print("\nsite=" + site);
                System.err.print("\nbytes=" + bytes);
                System.err.print("\ncontent=" + content);

                toRecordMap.put(new Unique(bytes), val.toString());
            }

            // If we have more than ten records, remove the one with the lowest number of bytes
            // As this tree map is sorted in descending order, the site with
            // the lowest number of bytes is the last key.
            Iterator<Map.Entry<Unique, String>> iter = toRecordMap.entrySet().iterator();
            while (toRecordMap.size() > TOP_SIZE) {
                iter.next();
                iter.remove();
            }

            //just to reverse the order from ascending to descending
            Map<Unique, String> newMap = new TreeMap<Unique, String>(Collections.reverseOrder());
            newMap.putAll(toRecordMap);

            for (String val : newMap.values()) {
                String[] splitValue = val.split(":", 3);
                String url = splitValue[0];
                String bytesStr = splitValue[1];
                String content = splitValue[2];
                context.write(new Text(url), new Text(bytesStr + ":" + content));
            }
        }
    }
}


