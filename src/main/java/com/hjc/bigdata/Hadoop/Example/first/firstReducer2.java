package com.hjc.bigdata.Hadoop.Example.first;

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
 * 		hjc-a.txt\t3
 * 		hjc-b.txt\t3
 * 2.输出
 * 		hjc,a.txt\t3 b.txt\t3
 */
public class firstReducer2 extends Reducer <Text, Text, Text, Text> {

    private Text out_value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {

            sb.append(value.toString() + " ");

        }

        out_value.set(sb.toString());

        context.write(key, out_value);

    }
}
