package com.kkb.mr.demo08.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {

    private FlowSortBean flowSortBean;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        flowSortBean.setPhone(fields[0]);
        flowSortBean.setUpFlow(Integer.parseInt(fields[1]));
        flowSortBean.setDownFlow(Integer.parseInt(fields[2]));
        flowSortBean.setUpCountFlow(Integer.parseInt(fields[3]));
        flowSortBean.setDownCountFlow(Integer.parseInt(fields[4]));

        context.write(flowSortBean, NullWritable.get());
    }
}
