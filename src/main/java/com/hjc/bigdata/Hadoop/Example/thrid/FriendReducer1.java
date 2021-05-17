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
 * keyin-valuein : （友:用户）
 * 					（c:A）,(C:B),(C:E)
 * 	reduce():
 * 	keyout-valueout  ：（友：用户，用户，用户，用户）
 */
public class FriendReducer1 extends Reducer <Text, Text, Text, Text> {

    private Text Out_value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer stringBuffer = new StringBuffer();

        for (Text value : values) {
            stringBuffer.append(value.toString() + ",");
        }

        Out_value.set(stringBuffer.toString());

        context.write(key, Out_value);

    }
}
