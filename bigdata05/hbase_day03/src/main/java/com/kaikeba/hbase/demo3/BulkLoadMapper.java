package com.kaikeba.hbase.demo3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(parts[0].getBytes());

        Put put = new Put(parts[0].getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), parts[1].getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), parts[2].getBytes());

        context.write(immutableBytesWritable, put);
    }
}
