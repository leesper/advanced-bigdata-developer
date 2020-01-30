package com.kkb.mr.demo09.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text kout;
    private IntWritable vout;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        kout = new Text();
        vout = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");

        for (String word : words) {
            kout.set(word);
            vout.set(1);
            context.write(kout, vout);
        }
    }
}
