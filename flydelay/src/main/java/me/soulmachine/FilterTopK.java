package me.soulmachine;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.TreeMap;

public class FilterTopK {

    public static class TopKMapper extends Mapper<Text,Text,NullWritable,Text> {

        private static final int K = 100;
        private TreeMap<Integer, Text> topTreeMap = new TreeMap<Integer, Text>();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            topTreeMap.put(Integer.parseInt(value.toString()), new Text(key.toString() + "\t" +value.toString()));
            if (topTreeMap.size() > K) {
                topTreeMap.remove(topTreeMap.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : topTreeMap.values()) {
                context.write(NullWritable.get(),t);
            }
        }

    }

    public static class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private static final int K = 100;
        private TreeMap<Integer, Text> topTreeMap = new TreeMap<Integer, Text>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {

                String[] tokens = value.toString().split("\t");
                Integer avg = Integer.parseInt(tokens[1]);
                topTreeMap.put(avg, new Text(value));
                if (topTreeMap.size() > K) {
                    topTreeMap.remove(topTreeMap.firstKey());
                }
            }
            for (Text t : topTreeMap.descendingMap().values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

}
