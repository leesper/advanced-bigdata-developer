package com.kaikeba.hbase.demo1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ReadMapper extends TableMapper<Text, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] rowkey_bytes = key.get();
        String rowkey = Bytes.toString(rowkey_bytes);
        Text text = new Text(rowkey);

        Put put = new Put(rowkey_bytes);
        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            byte[] family_bytes = CellUtil.cloneFamily(cell);
            String family = Bytes.toString(family_bytes);
            if ("f1".equals(family)) {
                byte[] qualifier_bytes = CellUtil.cloneQualifier(cell);
                String qualifier = Bytes.toString(qualifier_bytes);
                if ("name".equals(qualifier) || "age".equals(qualifier)) {
                    put.add(cell);
                }
            }
        }
        if (!put.isEmpty()) {
            context.write(text, put);
        }
    }
}
