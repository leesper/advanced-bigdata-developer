package com.kkb.mr.demo08.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private String phone;
    private Integer upFlow;
    private Integer downFlow;
    private Integer upCountFlow;
    private Integer downCountFlow;

    // used by deserialization
    public FlowSortBean() {}

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.upCountFlow = dataInput.readInt();
        this.downCountFlow = dataInput.readInt();
    }

    @Override
    public String toString() {
        return phone + "\t" + upFlow + "\t" +downFlow + "\t" + upCountFlow + "\t" + downCountFlow;
    }

    @Override
    public int compareTo(FlowSortBean o) {
        int i = this.downFlow.compareTo(o.downFlow);
        if (i == 0) {
            i = this.upCountFlow.compareTo(o.upCountFlow);
        }
        return i;
    }
}
