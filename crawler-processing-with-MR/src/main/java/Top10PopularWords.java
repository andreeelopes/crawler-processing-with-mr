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
import utils.WarcFileInputFormat;
import utils.WarcRecord;
import utils.WritableFilterLatinMap;
import utils.WritableWarcRecord;

import java.io.IOException;


public class Top10PopularWords {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, LongWritable, WritableFilterLatinMap> {

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont)
                throws IOException, InterruptedException { //TODO is it better only map adding an attribute?

            WarcRecord val = value.getRecord();

//            boolean isLatinAlphabet = val.getContentUTF8().matches("");// TODO
//            IntWritable isLatinAlphabetInt = isLatinAlphabet ? new IntWritable(1) : new IntWritable(0);
//
//
//            try {
//                cont.write(key, new WritableFilterLatinMap(value, isLatinAlphabetInt));
//
//            } catch (Exception e) {
//            }
        }

    }


    public static class MyReduce extends Reducer<Text, WritableFilterLatinMap, Text, WritableWarcRecord> {

        public void reduce(Text key, Iterable<WritableFilterLatinMap> values, Context cont)
                throws IOException, InterruptedException {


        }

    }


    public static void main(String[] args) throws Exception {
//        Job conf = Job.getInstance(new Configuration(), "filter-latin");
//        conf.setJarByClass(FilterLatin.class);
//
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(WritableWarcRecord.class);
//
//        conf.setMapperClass(MyMap.class);
//        conf.setCombinerClass(MyReduce.class); //TODO useless
//        conf.setReducerClass(MyReduce.class); //TODO useless
//
//        conf.setInputFormatClass(WarcFileInputFormat.class);
////        conf.setOutputFormatClass(WarcFileInputFormat.class); // TODO how to output the same type as input
//
//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//
//        conf.waitForCompletion(true); // submit and wait
    }


}
