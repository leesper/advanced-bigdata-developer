package com.kaikeba.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");

        int res = ToolRunner.run(configuration, new HBaseMain(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(HBaseMain.class);

        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("myuser"), new Scan(), ReadMapper.class,
                Text.class, Put.class, job);

        TableMapReduceUtil.initTableReducerJob("myuser2", WriteReducer.class, job);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }
}
