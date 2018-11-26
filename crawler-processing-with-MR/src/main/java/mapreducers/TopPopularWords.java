package mapreducers;

import org.apache.hadoop.io.IntWritable;
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


public class TopPopularWords {

    private static final int TOP_SIZE = 10;

    public static class MyMap extends Mapper<Text, Text, NullWritable, Text> {

        private static TreeMap<Unique, String> toRecordMap =
                new TreeMap<Unique, String>(new UniqueComparator()); // (count, word:count)

        private Text result = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(Text key, Text value, Context cont) { // (word,count)

            toRecordMap.put(new Unique(Integer.parseInt(value.toString())), key.toString() + ":" + value);

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
                result.set(val);
                context.write(NullWritable.get(), result);
            }
        }

    }


    public static class MyReduce extends Reducer<NullWritable, Text, Text, IntWritable> { // (null, word:count)

        private static TreeMap<Unique, String> toRecordMap =
                new TreeMap<Unique, String>(new UniqueComparator()); //(null, word:count)

        private Text wordText = new Text();
        private IntWritable countWritable = new IntWritable();

        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {// val = (word:count)
                System.err.print("\n---DEBUG-Reducer---\nval=" + val.toString());
                String[] splittedValue = val.toString().split(":", 2);

                String word = splittedValue[0];
                int count = Integer.parseInt(splittedValue[1]);

                System.err.print("\nword=" + word);
                System.err.print("\ncount=" + count);

                toRecordMap.put(new Unique(count), val.toString());
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
                String[] splittedValue = val.split(":", 2);
                String word = splittedValue[0];
                int count = Integer.parseInt(splittedValue[1]);

                wordText.set(word);
                countWritable.set(count);
                context.write(wordText, countWritable);
            }

        }

    }

}
