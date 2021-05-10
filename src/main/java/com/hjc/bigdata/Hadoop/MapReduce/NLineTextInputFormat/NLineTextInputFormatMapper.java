package com.hjc.bigdata.Hadoop.MapReduce.NLineTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:35
 * @Version : 1.0
 */
public class NLineTextInputFormatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text out_key = new Text();
    private IntWritable out_value = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            out_key.set(word);
            context.write(out_key, out_value);
        }
    }
}
