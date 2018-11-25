package mapReducers;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.*;

import java.io.IOException;
import java.net.URL;


public class LatinSitesNetPerformance {


    public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

        private Text site = new Text();

        protected void setup(Context cont) {
            System.err.println(">>>Processing>>> " + ((FileSplit) cont.getInputSplit()).getPath().toString());
        }


        public void map(LongWritable key, WritableWarcRecord value, Context cont) {

            WarcRecord val = value.getRecord();

            String bytesString = val.getHeaderMetadataItem("Content-Length");
            String url = val.getHeaderMetadataItem("WARC-Target-URI");
            String content = val.getContentUTF8()
                    .replaceAll("\n", " ")
                    .replaceAll("\r", " ")
                    .replaceAll("\\s+", " ")
                    .replaceAll("[^a-zA-Z]+", " ");

            try {
                if (isLatinAlphabet(content)) {
                    site.set(new URL(url).getHost());
                    cont.write(site, new Text(bytesString + ":" + content));
                }
            } catch (Exception e) {
                //does not have url
            }
        }

        //TODO improve
        private boolean isLatinAlphabet(String text) {
            if (text == null)
                return false;
            text = text.replaceAll("\\p{Punct}", "")
                    .trim()
                    .replaceAll(" ", "")
                    .replaceAll("[^\\p{L}\\p{Nd}]+", "")
                    .replaceAll("[ºª]", "");

            System.out.print("\nafter filter = " + text + "\n");

            return text.matches("[A-Za-z]+");
        }


    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context cont)
                throws IOException, InterruptedException {

            int bytes = 0;
            String content = "";


            for (Text val : values) {
                if (val != null) {
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
