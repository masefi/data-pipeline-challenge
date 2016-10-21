package me.soulmachine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import me.soulmachine.DelayCount.IntMeanReducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import me.soulmachine.DelayCount.TokenizerMapper;

public class DelayCountTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        TokenizerMapper mapper = new TokenizerMapper();
        IntMeanReducer reducer = new IntMeanReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Object(), new Text("2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA"));
        mapDriver.withOutput(new Text("ATL-PHX"), new IntWritable(7));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
            List<IntWritable> values = new ArrayList<IntWritable>();
            values.add(new IntWritable(1));
            values.add(new IntWritable(1));
            reduceDriver.withInput(new Text("ATL-PHX"), values);
            reduceDriver.withOutput(new Text("ATL-PHX"), new IntWritable(2));
            reduceDriver.runTest();
    }
    @Test
    public void testMapReduce() throws IOException{
        mapReduceDriver.withInput(new Object(), new Text(
                "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA"));
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        mapReduceDriver.withOutput(new Text("ATL-PHX"), new IntWritable(2));
        mapReduceDriver.runTest();
    }

}

