package com.hjc.bigdata.Hadoop.MapReduce.Partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:43
 * @Version : 1.0
 */
/*
 * 1. 统计手机号(String)的上行(long,int)，下行(long,int)，总流量(long,int)
 *
 * 手机号为key,Bean{上行(long,int)，下行(long,int)，总流量(long,int)}为value
 *
 */
public class FlowbeanMapper extends Mapper<LongWritable, Text, Text, Flowbean> {

    private Text out_key = new Text();
    private Flowbean out_value = new Flowbean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] word = value.toString().split("\t");

        //封装手机号
        out_key.set(word[1]);

        //封装上行流量和下行流量
        out_value.setUpFlow(Long.parseLong(word[word.length - 3]));
        out_value.setDownFlow(Long.parseLong(word[word.length - 2]));

        context.write(out_key, out_value);

    }
}