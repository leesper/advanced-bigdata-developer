package com.kkb.mr.demo04.keyvalue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable> {
    private LongWritable count = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, count);
    }
}
