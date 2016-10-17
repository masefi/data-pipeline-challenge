package me.soulmachine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DelayCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private IntWritable arrDelay = new IntWritable(0);
        private Text route = new Text();

        private static final int arrDelayCol = 14;
        private static final int sourceCol = 16;
        private static final int destCol = 17;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //skip the headers
            try {
                if (value.toString().contains("Year") && value.toString().contains("ActualElapsedTime")) return;
            } catch (Exception e) {
                e.printStackTrace();
            }

            String[] tokens = value.toString().split(",");

            // check if there are 29 columns in each input line
            if (tokens.length != 29) return;;

            // check the arrival delay format
            try {
                Integer.parseInt(tokens[arrDelayCol]);
            } catch(NumberFormatException e) {
                return;
            }
            // check src and destination are present
            if ( tokens[sourceCol] == null  || tokens[sourceCol] == "NA" || tokens[destCol] == null || tokens[destCol] == "NA") return;

            String routeString = tokens[16] + "-" + tokens[17];
            route.set(routeString);
            int delay = Integer.parseInt(tokens[14]);
            arrDelay.set(delay);
            context.write(route, arrDelay);
        }
    }

    public static class IntMeanReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "delay count");
        job.setJarByClass(DelayCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntMeanReducer.class);
        job.setReducerClass(IntMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
