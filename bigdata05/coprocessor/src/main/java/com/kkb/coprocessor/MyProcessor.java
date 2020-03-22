package com.kkb.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

public class MyProcessor extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);

        List<Cell> cells = put.get("info".getBytes(), "name".getBytes());
        Cell nameCell = cells.get(0);

        Table proc2 = connection.getTable(TableName.valueOf("proc2"));
        Put put1 = new Put(put.getRow());
        put1.add(nameCell);

        proc2.put(put1);
        proc2.close();
        connection.close();
    }
}
