package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.FlowbeanSort2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/11; 21:22
 * @Version : 1.0
 */
public class FlowbeanSort2Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private LongWritable out_key = new LongWritable();
    private Text out_value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");

        //封装总流量为key
        out_key.set(Long.parseLong(values[3]));

        out_value.set(values[0] + "\t" + values[1] + "\t" + values[2] + "\t");

        context.write(out_key, out_value);

    }
}