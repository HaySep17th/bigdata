package com.hjc.bigdata.Hadoop.MapReduce.CustomOutputFormat.example1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 12:11
 * @Version : 1.0
 */
public class MyCustomOutputFormat extends FileOutputFormat<String, NullWritable> {

    @Override
    public RecordWriter<String, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException,
            InterruptedException {
        return new MyRecordWriter(job);
    }
}
