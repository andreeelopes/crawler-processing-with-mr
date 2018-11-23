import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.WarcFileInputFormat;
import utils.WarcRecord;
import utils.WritableWarcRecord;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont)
                throws IOException, InterruptedException {

            WarcRecord val = value.getRecord();
            String content = val.getContentUTF8();

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
        Job conf = Job.getInstance(new Configuration(), "wordcount");
        conf.setJarByClass(WordCount.class);

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
