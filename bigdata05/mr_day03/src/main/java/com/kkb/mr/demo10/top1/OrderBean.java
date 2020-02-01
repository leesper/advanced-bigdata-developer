package com.kkb.mr.demo10.top1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderID;
    private Double price;

    @Override
    public int compareTo(OrderBean o) {
        int orderIDCompare = this.orderID.compareTo(o.orderID);
        if (orderIDCompare == 0) {
            int priceCompare = this.price.compareTo(o.price);
            return -priceCompare;
        } else {
            return orderIDCompare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderID);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderID = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderID + "\t" +  price;
    }
}
