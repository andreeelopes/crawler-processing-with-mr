import mapreducers.NetworkPerformance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.parsing.WarcFileInputFormat;

public class NetworkPerformanceProcessing {

    public static void main(String[] args) throws Exception {
        Job conf = Job.getInstance(new Configuration(), "network-performance");
        conf.setJarByClass(NetworkPerformance.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(NetworkPerformance.MyMap.class);
        conf.setReducerClass(NetworkPerformance.MyReduce.class);

        conf.setInputFormatClass(WarcFileInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.waitForCompletion(true); // submit and wait
    }

}
