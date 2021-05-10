package com.hjc.bigdata.Hadoop.MapReduce.Flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/10; 21:43
 * @Version : 1.0
 */
public class FlowbeanReducer extends Reducer<Text, Flowbean, Text, Flowbean> {

    private Flowbean out_value = new Flowbean();

    @Override
    protected void reduce(Text key, Iterable<Flowbean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (Flowbean flowbean : values) {
            sumUpFlow += flowbean.getUpFlow();
            sumDownFlow += flowbean.getDownFlow();
        }

        out_value.setUpFlow(sumUpFlow);
        out_value.setDownFlow(sumDownFlow);
        out_value.setSumFlow(sumUpFlow + sumDownFlow);

        context.write(key, out_value);
    }
}