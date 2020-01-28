package com.kkb.mr.demo04.keyvalue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        count = new LongWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        for (LongWritable value : values) {
            total += value.get();
        }
        count.set(total);
        context.write(key, count);
    }
}
