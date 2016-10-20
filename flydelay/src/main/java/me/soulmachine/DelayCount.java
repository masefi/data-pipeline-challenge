package me.soulmachine;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



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
            int cma = 0;
            long cmr = 0;
            long addendum;
            long n_values = 0;
            for (IntWritable val : values) {
                ++n_values;
                addendum = val.get() - cma + cmr;
                cma += addendum / n_values;
                cmr = addendum % n_values;
            }
            result.set(cma);
            context.write(key, result);
        }
    }

}
