package com.hjc.bigdata.Hadoop.Example.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 20:37
 * @Version : 1.0
 */

/*
 * 1.输入
 * 		hjc yhy
 * 2.输出
 * 		hjc-a.txt,1
 *      yhy-a.txt,1
 */

public class firstMapper1 extends Mapper <LongWritable, Text, Text, IntWritable> {

    private Text out_key = new Text();
    private IntWritable out_value = new IntWritable(1);
    private String filename;

    /*
    setup()方法在map()方法运行之前就会运行一次，而且只运行一次。所以使用这个方法获取切片的文件名。
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        filename = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split(" ");

        for (String word : values) {
            out_key.set(word + "-" + filename);
            context.write(out_key, out_value);
        }

    }
}
