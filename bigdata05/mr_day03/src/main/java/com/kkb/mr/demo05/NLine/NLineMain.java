package com.kkb.mr.demo05.NLine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class NLineMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf  = super.getConf();
        Job job = Job.getInstance(conf, "NLine");
        job.setJarByClass(NLineMain.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        NLineInputFormat.addInputPath(job, new Path(strings[0]));

        job.setMapperClass(NLineMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(NLineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int code = ToolRunner.run(conf, new NLineMain(), args);
        System.exit(code);
    }

    public static class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0L;
            for (LongWritable value : values) {
                result += value.get();
            }
            context.write(key, new LongWritable(result));
        }
    }
}
