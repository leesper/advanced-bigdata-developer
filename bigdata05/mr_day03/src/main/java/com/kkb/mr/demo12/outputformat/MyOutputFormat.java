package com.kkb.mr.demo12.outputformat;

import com.kkb.mr.demo06.inputformat.MyRecordReader;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    private static String good;
    private static String other;

    public static void setGood(String g) {
        good = g;
    }

    public static void setOther(String o) {
        other = o;
    }

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        Path goodComment = new Path(good);
        Path otherComment = new Path(other);
        FSDataOutputStream goodOutputStream = fs.create(goodComment);
        FSDataOutputStream otherOutputStream = fs.create(otherComment);
        return new MyRecordWriter(goodOutputStream, otherOutputStream);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
        FSDataOutputStream goodStream = null;
        FSDataOutputStream otherStream = null;

        public MyRecordWriter(FSDataOutputStream good, FSDataOutputStream other) {
            goodStream = good;
            otherStream = other;
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            if (text.toString().split("\t")[9].equals("0")) {
                goodStream.write(text.toString().getBytes());
                goodStream.write("\n".getBytes());
            } else {
                otherStream.write(text.toString().getBytes());
                otherStream.write("\n".getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (goodStream != null) {
                goodStream.close();
                goodStream = null;
            }

            if (otherStream != null) {
                otherStream.close();
                otherStream = null;
            }
        }
    }
}
