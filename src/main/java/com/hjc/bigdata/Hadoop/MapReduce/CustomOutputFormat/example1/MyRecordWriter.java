package com.hjc.bigdata.Hadoop.MapReduce.CustomOutputFormat.example1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 12:32
 * @Version : 1.0
 */
public class MyRecordWriter extends RecordWriter<String, NullWritable> {

    private Path atguiguPath = new Path("/Users/apple/Downloads/hadoop/output/outputformat/example1/atguigu.log");
    private Path otherPath = new Path("/Users/apple/Downloads/hadoop/output/outputformat/example1/other.log");

    private FSDataOutputStream atguiguOS;
    private FSDataOutputStream otherOS;

    private FileSystem fs;

    public MyRecordWriter(TaskAttemptContext job) throws IOException{

        Configuration conf = job.getConfiguration();

        fs = FileSystem.get(conf);

        atguiguOS = fs.create(atguiguPath);
        otherOS = fs.create(otherPath);

    }

    @Override
    public void write(String key, NullWritable value) throws IOException, InterruptedException {

        if (key.contains("atguigu")) {
            atguiguOS.write(key.getBytes());
        } else {
            otherOS.write(key.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (atguiguOS != null) {
            IOUtils.closeStream(atguiguOS);
        }

        if (otherOS != null) {
            IOUtils.closeStream(otherOS);
        }

        if (fs != null) {
            fs.close();
        }
    }
}
