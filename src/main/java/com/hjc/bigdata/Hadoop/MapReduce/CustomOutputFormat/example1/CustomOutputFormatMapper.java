package com.hjc.bigdata.Hadoop.MapReduce.CustomOutputFormat.example1;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/16; 12:11
 * @Version : 1.0
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 需求：
 *  过滤输入的日志文件，将包含atguigu的网站输出到一个结果文件中，其他的输出到另一个结果文件中
 */
public class CustomOutputFormatMapper extends Mapper <LongWritable, Text, String, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String values = value.toString();

        context.write(values + "\r\n", NullWritable.get());

    }
}
