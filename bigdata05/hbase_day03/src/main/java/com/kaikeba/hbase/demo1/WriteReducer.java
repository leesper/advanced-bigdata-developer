package com.kaikeba.hbase.demo1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WriteReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable keyout = new ImmutableBytesWritable();
        keyout.set(key.toString().getBytes());

        for (Put value : values) {
            context.write(keyout, value);
        }
    }
}
