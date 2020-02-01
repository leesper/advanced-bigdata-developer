package com.kkb.mr.demo10.top1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    public MyGroup() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean)a;
        OrderBean b1 = (OrderBean)b;

        return a1.getOrderID().compareTo(b1.getOrderID());
    }
}
