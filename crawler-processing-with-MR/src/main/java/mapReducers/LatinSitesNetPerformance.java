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
import java.text.Normalizer;


public class LatinSitesNetPerformance {



    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

        private static final String IS_ENGLISH_REGEX = "^[ \\w \\d \\s \\. \\& \\+ \\- \\, \\! \\@ \\# \\$ \\% \\^ \\* \\( \\) \\; \\\\ \\/ \\| \\< \\> \\\" \\' \\? \\= \\: \\[ \\] ]*$";

        private Text site = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }


        public void map(LongWritable key, WritableWarcRecord value, Context cont) {

            WarcRecord val = value.getRecord();

            String bytesString = val.getHeaderMetadataItem("Content-Length");
            String url = val.getHeaderMetadataItem("WARC-Target-URI");
            String content = val.getContentUTF8()
                    .replaceAll("\n" ," ")
                    .replaceAll("\r", " ");

            try {
                if (isLatinAlphabet(content)) {
                    site.set(new URL(url).getHost());
//                    if (content.length() > 5) //TODO remove this and substring
//                        cont.write(site, new Text(bytesString + ":" + content.substring(0, 3)));
                    cont.write(site, new Text(bytesString + ":" + content));
                }
            } catch (Exception e) {
                //does not have url
            }
        }


        private boolean isLatinAlphabet(String text) {
            if (text == null) {
                return false;
            }
            return text.matches("[ \\w]+");
        }


    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context cont)
                throws IOException, InterruptedException {

            int bytes = 0;
            String content = "";


            for (Text val : values) {
                if(val != null) {
                    String[] splittedVal = val.toString().split(":", 2);

                    bytes += Integer.parseInt(splittedVal[0]);
                    content += (splittedVal[1]);
                    content += " ";
                }
            }
            cont.write(key, new Text(String.valueOf(bytes) + ":" + content));
        }

    }
}
