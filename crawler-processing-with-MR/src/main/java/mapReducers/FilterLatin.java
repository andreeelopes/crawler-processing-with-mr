package mapReducers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class FilterLatin {

    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

        private Text word = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }

        private boolean isLatinAlphabet(String text) {

                return true;
        }

        public void map(LongWritable key, WritableWarcRecord value, Context cont) throws IOException { //TODO is it better only map adding an attribute?

            WarcRecord val = value.getRecord();

            String bytesString = val.getHeaderMetadataItem("Content-Length");
            String url = val.getHeaderMetadataItem("WARC-Target-URI");
            String content = val.getContentUTF8();
            try {
                if (content != null && isLatinAlphabet(content)) {
                    word.set(new URL(url).getHost());
                    cont.write(word, new Text(bytesString + ":" + content));
                }
            } catch (Exception e) {
                //throw new IOException("DEBUG_MAP : " + url + val.getContentUTF8().substring(0, 100));
            }
        }
    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context cont)
                throws IOException, InterruptedException {

            int bytes = 0;
            String content = "";


            for (Text val : values) {
                if (val != null) {
                    String[] splitedVal = val.toString().split(":", 2);
                    if (splitedVal[1].length() < 10)
                        content += (splitedVal[1].substring(0, (int) (splitedVal[1].length() * 0.10)));
                    else
                        content += (splitedVal[1].substring(0, 9));
                    bytes += Integer.parseInt(splitedVal[0]);
                    content += (" ");
                }
            }
            cont.write(key, new Text(String.valueOf(bytes) + ":" + content));
        }

    }
}
