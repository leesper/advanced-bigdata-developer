package com.kkb.mr.demo03.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word;
    private IntWritable count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        word = new Text();
        count = new IntWritable(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");

        for (String w : words) {
            word.set(w);
            context.write(word, count);
        }
    }
}
