package com.hjc.bigdata.Hadoop.MapReduce.KeyValueTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:41
 * @Version : 1.0
 */
public class KeyValueTextInputFormatMapper extends Mapper<Text, Text, Text, IntWritable> {

    private IntWritable out_value = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        context.write(key, out_value);

    }
}