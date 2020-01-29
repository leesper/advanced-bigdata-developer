package com.kkb.mr.demo07.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();
        int partition = 0;
        if (phone != null && phone != "") {
            if (phone.startsWith("135")) {
                partition = 0;
            } else if (phone.startsWith("136")) {
                partition = 1;
            } else if (phone.startsWith("137")) {
                partition = 2;
            } else if (phone.startsWith("138")) {
                partition = 3;
            } else if (phone.startsWith("139")) {
                partition = 4;
            } else {
                partition = 5;
            }
        } else {
            partition = 5;
        }
        return partition;
    }
}
