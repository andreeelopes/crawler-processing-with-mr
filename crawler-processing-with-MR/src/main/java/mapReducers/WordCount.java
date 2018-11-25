package mapReducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount {

    public static class MyMap extends Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(Text key, Text value, Context cont)
                throws IOException, InterruptedException {

            String[] splitValue = value.toString().split(":", 2); //(bytes, content)
            String content = splitValue[1];


            StringTokenizer itr = new StringTokenizer(content);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                if (word.getLength() > 5) {
                    word.set(word.toString().toLowerCase());
                    cont.write(word, one);
                }
            }


        }

    }


    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable count = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context cont)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            count.set(sum);
            cont.write(key, count);
        }

    }

}
