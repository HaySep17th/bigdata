package com.hjc.bigdata.Hadoop.Example.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 20:37
 * @Version : 1.0
 */
/*
 * 1.输入
 * 		hjc-a.txt,1
 * 		hjc-a.txt,1
 * 		hjc-a.txt,1
 * 2.输出
 * 		hjc-a.txt,3
 */
public class firstReducer1 extends Reducer <Text, IntWritable, Text, IntWritable> {

    private IntWritable out_value = new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        out_value.set(sum);

        context.write(key, out_value);

    }
}
