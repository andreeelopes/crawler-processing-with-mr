import mapReducers.LatinSitesNetPerformance;
import mapReducers.TopHeaviestSites;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import utils.WarcFileInputFormat;

public class TopWordFrequency extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        long beginTime = System.currentTimeMillis();

        JobControl jobControl = new JobControl("jobChain");

        Job job1 = Job.getInstance(new Configuration(), "Crawler Performance by site(latin alphabet only)");
        job1.setJarByClass(LatinSitesNetPerformance.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/latin"));

        job1.setMapperClass(LatinSitesNetPerformance.MyMap.class);
        job1.setReducerClass(LatinSitesNetPerformance.MyReduce.class);
//        job1.setCombinerClass(mapReducers.LatinSitesNetPerformance.MyReduce.class);

        job1.setInputFormatClass(WarcFileInputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        controlledJob1.setJob(job1);

        jobControl.addJob(controlledJob1);

        Job job2 = Job.getInstance(new Configuration(), "Top Heaviest Sites");

        job2.setJarByClass(TopHeaviestSites.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/latin"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/heaviest"));

        job2.setMapperClass(TopHeaviestSites.MyMap.class);
        job2.setReducerClass(TopHeaviestSites.MyReduce.class);
//        job2.setCombinerClass(TopHeaviestSites.MyReduce.class);

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);

//        Job job3 = Job.getInstance(new Configuration(), "Word Count");
//
//        job3.setJarByClass(mapReducers.WordCount.class);
//        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/heaviest"));
//        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/wordcount"));
//
//        job3.setMapperClass(mapReducers.WordCount.MyMap.class);
//        job3.setReducerClass(mapReducers.WordCount.MyReduce.class);
////        job3.setCombinerClass(mapReducers.WordCount.MyReduce.class);
//
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(IntWritable.class);
//
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(IntWritable.class);
//        job3.setInputFormatClass(KeyValueTextInputFormat.class);
//
//        ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());
//        controlledJob3.setJob(job3);
//
//        // make job3 dependent on job2
//        controlledJob3.addDependingJob(controlledJob2);
//        // add the job to the job control
//        jobControl.addJob(controlledJob3);
//
//
//        Job job4 = Job.getInstance(new Configuration(), "Top 10 Popular Words");
//
//        job4.setJarByClass(mapReducers.TopPopularWords.class);
//        FileInputFormat.setInputPaths(job4, new Path(args[1] + "/wordcount"));
//        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/topPopWords"));
//
//        job4.setMapperClass(mapReducers.TopPopularWords.MyMap.class);
//        job4.setReducerClass(mapReducers.TopPopularWords.MyReduce.class);
////        job2.setCombinerClass(mapReducers.TopPopularWords.MyReduce.class);
//
//        job4.setMapOutputKeyClass(NullWritable.class);
//        job4.setMapOutputValueClass(Text.class);
//
//        job4.setOutputKeyClass(Text.class);
//        job4.setOutputValueClass(IntWritable.class);
//        job4.setInputFormatClass(KeyValueTextInputFormat.class);
//
//        ControlledJob controlledJob4 = new ControlledJob(job4.getConfiguration());
//        controlledJob4.setJob(job4);
//
//        // make job4 dependent on job3
//        controlledJob4.addDependingJob(controlledJob3);
//        // add the job to the job control
//        jobControl.addJob(controlledJob4);



        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("----Elaped Time = " + (System.currentTimeMillis() - beginTime) + "(ms)-----");
            printJobControlStats(jobControl);
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }

        }
        System.out.println("----Elaped Time = " + (System.currentTimeMillis() - beginTime) + "(ms)-----");
        printJobControlStats(jobControl);
        System.out.println("Hasta la vista, baby!");


        System.exit(0);
        return (job1.waitForCompletion(true) ? 0 : 1);

    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopWordFrequency(), args);
        System.exit(exitCode);
    }


    private void printJobControlStats(JobControl jobControl) {
        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
    }

}