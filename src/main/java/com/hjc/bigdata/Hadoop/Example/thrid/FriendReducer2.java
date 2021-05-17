package com.hjc.bigdata.Hadoop.Example.thrid;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/17; 21:41
 * @Version : 1.0
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * keyin-valuein : (A-B,C),(A-B,E)
 * 	reduce():
 * 	keyout-valueout  ： (A-B:C,E)
 */
public class FriendReducer2 extends Reducer <Text, Text, Text, Text> {

    private Text out_value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer stringBuffer = new StringBuffer();

        for (Text value : values) {
            stringBuffer.append(value.toString() + ",");
        }

        out_value.set(stringBuffer.toString());

        context.write(key, out_value);
    }
}
