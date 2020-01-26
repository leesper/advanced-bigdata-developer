package com.kkb.mr.demo02.serialize;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone;
    private FlowBean flowBean;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        phone = new Text();
        flowBean = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phone = fields[1];
        String upFlow = fields[6];
        String downFlow = fields[7];
        String upCountFlow = fields[8];
        String downCountFlow = fields[9];
        this.phone.set(phone);
        this.flowBean.setUpFlow(Integer.parseInt(upFlow));
        this.flowBean.setDownFlow(Integer.parseInt(downFlow));
        this.flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        this.flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));
        context.write(this.phone, this.flowBean);
    }
}
