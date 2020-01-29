package com.kkb.mr.demo08.sort;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowSortMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "PhoneFlow");
        job.setJarByClass(FlowSortMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(strings[0]));

        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(FlowSortBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int resCode = ToolRunner.run(configuration, new FlowSortMain(), args);
        System.exit(resCode);
    }
}
