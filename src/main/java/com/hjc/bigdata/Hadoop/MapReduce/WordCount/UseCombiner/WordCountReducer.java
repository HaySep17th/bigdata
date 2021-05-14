package com.hjc.bigdata.Hadoop.MapReduce.WordCount.UseCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/14; 20:14
 * @Version : 1.0
 */
public class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    private IntWritable out_value = new IntWritable();

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
