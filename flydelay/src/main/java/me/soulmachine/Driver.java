package me.soulmachine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.NullWritable;

import me.soulmachine.FilterTopK.TopKReducer;
import me.soulmachine.FilterTopK.TopKMapper;
import me.soulmachine.DelayCount.IntMeanReducer;
import me.soulmachine.DelayCount.TokenizerMapper;

public class Driver extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        // first job
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "DelayCount");
        job1.setJarByClass(DelayCount.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntMeanReducer.class);
        job1.setReducerClass(IntMeanReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
        job1.waitForCompletion(true);

        //Second Job
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "FilterTopK");
        job2.setJarByClass(FilterTopK.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(TopKMapper.class);
        job2.setCombinerClass(TopKReducer.class);
        job2.setReducerClass(TopKReducer.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
        return job2.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
            System.exit(0);
        }
        ToolRunner.run(new Configuration(), new Driver(), args);
    }
}