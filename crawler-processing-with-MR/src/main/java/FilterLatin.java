import javafx.beans.value.WritableBooleanValue;
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
import utils.*;

import java.io.IOException;
import java.net.URL;
import java.util.StringTokenizer;


public class FilterLatin {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

        private Text word = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont) { //TODO is it better only map adding an attribute?

            WarcRecord val = value.getRecord();

            boolean isLatinAlphabet = true;

            String bytesString = val.getHeaderMetadataItem("Content-Length");
            String url = val.getHeaderMetadataItem("WARC-Target-URI");

            try {

                if (isLatinAlphabet) {
                    word.set(new URL(url).getHost());
                    cont.write(word, new Text(bytesString + " " + new String(val.getContent())));
                }
            } catch (Exception e) {
                System.err.println(url);
            }
        }

    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context cont)
                throws IOException, InterruptedException {

            int bytes = 0;
            StringBuilder content = new StringBuilder("");

            for (Text val : values) {

                try {
                    StringTokenizer st = new StringTokenizer(val.toString());
                    if (st.hasMoreTokens()) {
                        bytes += Integer.getInteger(st.nextToken());
                    }

                    while (st.hasMoreTokens()) {
                        content.append(" ");
                        content.append(st.nextToken());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("DEBUG = " + val.toString());
                }

            }
            cont.write(key, new Text(String.valueOf(bytes) + ":" + content));
        }

    }


    public static void main(String[] args) throws Exception {
        Job conf = Job.getInstance(new Configuration(), "filter-latin plus site performance");
        conf.setJarByClass(FilterLatin.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MyMap.class);
        conf.setCombinerClass(MyReduce.class); //TODO useless
        conf.setReducerClass(MyReduce.class); //TODO useless

        conf.setInputFormatClass(WarcFileInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class); // TODO how to output the same type as input

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.waitForCompletion(true); // submit and wait
    }


}
