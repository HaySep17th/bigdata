package com.hjc.bigdata.Hadoop.Example.second;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 21:20
 * @Version : 1.0
 */
public class FlowBeanMapper extends Mapper <LongWritable, Text, LongWritable, Text> {

    private LongWritable out_key = new LongWritable();
    private Text out_value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");

        out_key.set(Long.parseLong(words[3]));

        out_value.set(words[0] + "\t" + words[1] + "\t" + words[2]);

        context.write(out_key, out_value);
    }
}
