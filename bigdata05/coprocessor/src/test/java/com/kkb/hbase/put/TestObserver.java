package com.kkb.hbase.put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

public class TestObserver {
    @Test
    public void testPut() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1,node2");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table proc1 = connection.getTable(TableName.valueOf("proc1"));
        Put put1 = new Put(Bytes.toBytes("hello_world"));

        put1.addColumn(Bytes.toBytes("info"), "name".getBytes(), "helloworld".getBytes());
        put1.addColumn(Bytes.toBytes("info"), "gender".getBytes(), "abc".getBytes());
        put1.addColumn(Bytes.toBytes("info"), "nationality".getBytes(), "test".getBytes());

        proc1.put(put1);
        byte[] row = put1.getRow();
        System.out.println(Bytes.toString(row));
        proc1.close();
        connection.close();
    }
}
