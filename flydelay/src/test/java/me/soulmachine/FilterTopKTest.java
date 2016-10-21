package me.soulmachine;

import org.apache.hadoop.io.NullWritable;

import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.io.Text;

import me.soulmachine.FilterTopK.*;

public class FilterTopKTest {

    MapDriver<Text,Text,NullWritable,Text> mapDriver;
    ReduceDriver<NullWritable, Text, NullWritable, Text> reduceDriver;
    MapReduceDriver<Text,Text,NullWritable,Text, NullWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        TopKMapper mapper = new TopKMapper();
        TopKReducer reducer = new TopKReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text("ATL-PHX"), new Text("10"));
        mapDriver.withOutput(NullWritable.get() , new Text("ATL-PHX 10"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
        List<Text> values = new ArrayList<Text>();
        reduceDriver.withInput(NullWritable.get(), values);
        reduceDriver.withOutput(NullWritable.get(), new Text("ATL-PHX 10"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException{
        mapReduceDriver.withInput(new Text("ATL-PHX"), new Text("1"));
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("1"));
        mapReduceDriver.withOutput(NullWritable.get(), new Text("ATL-PHX 10"));
        mapReduceDriver.runTest();
    }
}
