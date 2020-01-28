package com.kkb.mr.demo03.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text word;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        word = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        word.set(key);
        for (IntWritable value : values) {
            total += value.get();
        }
        context.write(word, new IntWritable(total));
    }
}
