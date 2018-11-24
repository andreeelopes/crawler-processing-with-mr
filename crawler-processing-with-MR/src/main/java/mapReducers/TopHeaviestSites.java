package mapReducers;

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
            /*
                ---DEBUG---
                key = 01202.ru
                value = 6915:Проститут
             */
            System.err.println("\n---DEBUG---\nkey = " + key.toString() + "\nvalue = " + value.toString() + "\n");


            if(key.toString().length() == 0 || value.toString().length() == 0)
                return;


            String[] splitedValue = value.toString().split(":", 2);
            long bytes = Long.parseLong(splitedValue[0]);

            System.err.println("\n---DEBUG---\nbytes=" + bytes + "\ncontent=" + splitedValue[1]);

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
                String[] splitedValue = val.toString().split(":", 3);

                String site = splitedValue[0];
                long bytes = Long.parseLong(splitedValue[1]);
                String content = splitedValue[2];
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
                String[] splitedValue = val.split(":", 3);
                String url = splitedValue[0];
                String content = splitedValue[2];
                context.write(new Text(url), new Text(content));
            }
        }
    }
}


