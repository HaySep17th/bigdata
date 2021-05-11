package com.hjc.bigdata.Hadoop.MapReduce.FlowbeanSort1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/11; 20:54
 * @Version : 1.0
 */
public class FlowbeanSort1Reducer extends Reducer <LongWritable, Text, Text, LongWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }

    }
}
