package com.hjc.bigdata.Hadoop.MapReduce.CostomInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:47
 * @Version : 1.0
 */
/*
 * 1. 改变切片策略，一个文件固定切1片，通过指定文件不可切
 *
 * 2. 提供RR ，这个RR读取切片的文件名作为key,读取切片的内容封装到bytes作为value
 */
public class MyInputFormat extends FileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        return new MyRecordReader();
    }

    //重写isSplitable，设置文件不可切
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
