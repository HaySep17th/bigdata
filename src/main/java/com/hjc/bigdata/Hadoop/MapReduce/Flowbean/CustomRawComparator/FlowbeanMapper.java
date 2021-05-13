package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.CustomRawComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/13; 20:58
 * @Version : 1.0
 */
public class FlowbeanMapper extends Mapper <LongWritable, Text, Flowbean, Text> {

    private Flowbean out_key = new Flowbean();
    private Text out_value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");

        out_key.setUpFlow(Long.parseLong(words[1]));
        out_key.setDownFlow(Long.parseLong(words[2]));
        out_key.setSumFlow(Long.parseLong(words[3]));

        out_value.set(words[0]);

        context.write(out_key, out_value);

    }
}
