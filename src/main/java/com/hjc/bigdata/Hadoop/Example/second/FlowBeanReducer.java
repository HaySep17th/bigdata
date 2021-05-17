package com.hjc.bigdata.Hadoop.Example.second;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 21:21
 * @Version : 1.0
 */
public class FlowBeanReducer extends Reducer <LongWritable, Text, Text, LongWritable> {

    private int num = 0;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        //只输出前十条
        for (Text value : values) {

            if (num >= 10) {
                return ;
            }

            context.write(value, key);

            num++;
        }

    }
}
