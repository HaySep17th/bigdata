package com.hjc.bigdata.Hadoop.MapReduce.Flowbean.CustomRawComparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/13; 21:00
 * @Version : 1.0
 */
public class FlowbeanReducer extends Reducer<Flowbean, Text, Text, Flowbean> {

    @Override
    protected void reduce(Flowbean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }

    }
}
